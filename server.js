import express from 'express';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import cors from 'cors';
import { google } from 'googleapis';
import bodyParser from 'body-parser';
import pLimit from 'p-limit'; 

// dotenv.config();

const app = express();
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));

const port = process.env.PORT || 3000;

const corsOptions = {
  origin: 'https://phenomenal-kringle-da3979.netlify.app',
  optionsSuccessStatus: 200
};

// --- Настройка лимита Rate Limit ---
const CONCURRENCY_LIMIT = 1; // Одновременно 1 активных запросов к API Listok CRM
const limit = pLimit(CONCURRENCY_LIMIT);

app.use(cors(corsOptions));

app.use(express.json());

let accessToken = null;
let refreshToken = null;
let allSources = [];
let allPasses = []
let startD = null
let endD = null
let excludedPassesNames = [
  "Пробный абонемент (Никитина) (восстановлен 22.09.2025 10:15)",
  "Пробный абонемент (общий)",
  "Пробная персональная тренировка (общий)",
  "Отработка"
]
// Функция для авторизации в Google Sheets API
async function authorize() {
  // Проверяем, есть ли JSON в переменной окружения
  const credentials = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
  if (!credentials) {
    throw new Error("GOOGLE_APPLICATION_CREDENTIALS_JSON is not set");
  }

  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(credentials), // Парсим JSON из переменной
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const client = await auth.getClient();
  google.options({ auth: client });
  return google.sheets({ version: 'v4' });
}
// Функция для записи данных в Google Таблицу
async function writeDataToSheet(spreadsheetId, data) {
  const sheets = await authorize();

  const request = {
    spreadsheetId: spreadsheetId,
    range: 'Sheet1!A1',
    valueInputOption: 'USER_ENTERED',
    resource: {
      values: data,
    },
  };

  try {
    const response = await sheets.spreadsheets.values.update(request);
    console.log(response.data);
    return response.data;
  } catch (err) {
    console.error(err);
    throw err;
  }
}

// Функция для получения источников из ListokCRM
async function getSourcesFromListokCRM(accessToken) {
  try {
    const response = await fetch('https://an8242.listokcrm.ru/api/external/v2/sources', {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
      }
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(`API Error: ${response.status} - ${errorData.error_description || errorData.error || 'Unknown error'}`);
    }

    const data = await response.json();
    let sources
    if (data && data.data && Array.isArray(data.data)) {
      sources = data.data.map(source => ({ id: source.source_id, name: source.name }));
      console.log('Источники из ListokCRM:', sources);
      return sources;
    } else {
      console.warn('Не удалось получить источники из ListokCRM или неверный формат данных.');
      return [];
    }
  } catch (error) {
    console.error('Error fetching sources:', error);
    return [];
  }
}

async function getAllPasses(accessToken) {
  let passes = []
     let page = 1;
    let hasMore = true;
    const PAGE_LIMIT = 50; // Предполагаемый лимит на странице
    const DELAY_MS = 500; // Пауза 500 мс между запросами страниц для обхода 429

    const headers = {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    };
    
    console.log("Starting pass loading with pagination...");
const baseURL = 'https://an8242.listokcrm.ru/api/external/v2'
     while (hasMore) {

        const url = `${baseURL}/passes?page=${page}`;
        
        try {
            const response = await fetch(url, {
                method: "GET",
                headers: headers,
            });

            if (response.status === 429) {
                // Если hit Rate Limit, ждем дольше и пробуем ту же страницу снова
                console.warn(`429 hit during pass pagination on page ${page}. Waiting 5 seconds...`);
                await sleep(5000); 
                continue; // Повторяем ту же страницу
            }

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Failed to fetch passes page ${page}. Status: ${response.status}. Response: ${errorText}`);
            }

            const data = await response.json();
            
            // Предполагаем стандартную структуру Listok CRM с полем 'data'
            const currentPagePasses = data.data || [];
            
            passes.push(...currentPagePasses);
            
            // Логика пагинации
            // Если количество полученных элементов меньше лимита страницы, это последняя страница
            if (currentPagePasses.length < PAGE_LIMIT) {
                hasMore = false;
            } else {
                page++;
                // Добавляем обязательную задержку между страницами, чтобы избежать 429
                console.log(`Successfully fetched page ${page - 1}. Total passes so far: ${passes.length}. Waiting ${DELAY_MS}ms...`);
                await sleep(DELAY_MS);
            }

        } catch (error) {
            console.error(`Final error fetching passes on page ${page}:`, error.message);
            hasMore = false;
            // Если произошла ошибка, мы возвращаем то, что успели собрать
            throw error; 
        }
    }
    
    console.log(`Passes loading complete. Total passes fetched: ${passes.length}`);
          console.log('Абонементы из ListokCRM:', passes);

    return passes;
}

app.post('/exchange-token', async (req, res) => {  
  const { code } = req.body;

  const client_id = process.env.LISTOKCRM_CLIENT_ID;
  const client_secret = process.env.LISTOKCRM_CLIENT_SECRET;
  const redirect_uri = process.env.LISTOKCRM_REDIRECT_URI;

  if (!code) {
    return res.status(400).json({ error: 'Code is required' });
  }

  try {
    const response = await fetch('https://an8242.listokcrm.ru/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code: code,
        client_id: client_id,
        client_secret: client_secret,
        redirect_uri: redirect_uri,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      console.error('ListokCRM error:', data);
      return res.status(response.status).json({ error: data.error_description || data.error || 'Unknown error' });
    }

    accessToken = data.access_token;
    refreshToken = data.refresh_token;
 
    console.log(allSources,'sdsfsdfdf')
    res.json({
      success: true,  
      accessToken: accessToken, 
      refreshToken: refreshToken,
      
  }); 
  } catch (error) {
    console.error('Error during token exchange:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


app.get('/get_passes', async (req, res) => {
  if (!accessToken) {
    return res.status(401).json({ error: 'Access token is missing. Please exchange the code first.' });
  }

  try {
    const isTokenValid = () => !!accessToken;

    if (!isTokenValid()) {
      console.log('Access token is invalid, refreshing token...');
      if (!refreshToken) {
        return res.status(401).json({ error: 'Refresh token is missing. Please authenticate again.' });
      }
      try {
        const newTokens = await refreshAccessToken(refreshToken);
        accessToken = newTokens.accessToken;
        refreshToken = newTokens.refreshToken;
        await getSourcesFromListokCRM(accessToken);
      } catch (refreshError) {
        console.error('Failed to refresh access token:', refreshError);
        return res.status(401).json({ error: 'Failed to refresh access token. Please authenticate again.' });
      }
    }

    const baseURL = 'https://an8242.listokcrm.ru/api/external/v2/contacts';
    let allContacts = [];
    let page = 1;
    let hasMore = true;

    while (hasMore) {
      const url = `${baseURL}?page=${page}`;
      try {
        const response = await fetch(url, {
          method: 'GET',
          headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
          }
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(`API Error: ${response.status} - ${errorData.error_description || errorData.error || 'Unknown error'}`);
        }

        const data = await response.json();

        if (data && data.data && data.data.length > 0) {
          allContacts = allContacts.concat(data.data);
          page++;
        } else {
          hasMore = false;
        }
      } catch (error) {
        console.error('Error fetching page:', page, error);
        hasMore = false;
      }
    }

    console.log(`Получено ${allContacts.length} контактов`);
    res.json(allContacts);

  } catch (error) {
    console.error('Error fetching all contacts:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Функция для получения списка записей для одного контакта
async function getListingsForContact(contactId, accessToken) {
  try {
    const url = `https://an8242.listokcrm.ru/api/external/v2/contacts/${contactId}/listings?page=1`; // TODO: Add other parameters if needed
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
      }
    });

    if (!response.ok) {
      const errorData = await response.json();
      console.error(`Error fetching listings for contact ${contactId}:`, errorData);
      return null; // Return null in case of error
    }

    const data = await response.json();
    return data;

  } catch (error) {
    console.error(`Error fetching listings for contact ${contactId}:`, error);
    return null; // Return null in case of error
  }
}
// Новый эндпоинт для получения отфильтрованных клиентов, запроса записей и записи в Google Sheets
app.post('/generate-report', async (req, res) => {
    const { startDate, endDate, source, branch } = req.body;
    
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        // Если клиент не прислал токен в заголовке, это 401
        return res.status(401).json({ error: 'Authorization token is missing or malformed.' });
    }
    const clientAccessToken = authHeader.split(' ')[1]; // Токен, который прислал клиент
    
    // ВНИМАНИЕ: Мы сохраним логику обновления токена, но будем обновлять ГЛОБАЛЬНЫЕ
    // токены (accessToken, refreshToken) только для простоты прототипа.

      let allContacts = [];
    let filteredContacts = [];

        allSources = await getSourcesFromListokCRM(clientAccessToken);
        allPasses = await getAllPasses(clientAccessToken)
    // Переменная для работы в текущем запросе
    let currentAccessToken = clientAccessToken;

    try {
        let allContacts = [];
        let page = 1;
        let hasMore = true;
        let retryCount = 0;
        const MAX_RETRIES = 1; // Попытка обновить токен один раз
        const baseURL = 'https://an8242.listokcrm.ru/api/external/v2/contacts'; 

        while (hasMore) {
            const url = `${baseURL}?page=${page}`;
            let success = false;
            
            // Запускаем цикл попыток (одна попытка, плюс одна попытка после обновления токена)
            while (!success && retryCount <= MAX_RETRIES) {
                try {
                    const response = await fetch(url, {
                        method: 'GET',
                        headers: {
                            'Authorization': `Bearer ${currentAccessToken}`, // Используем токен клиента
                            'Content-Type': 'application/json',
                            'Accept': 'application/json',
                            'X-Requested-With': 'XMLHttpRequest'
                        }
                    });

                    if (response.status === 401) {
                        // Токен истек или невалиден:
                        if (retryCount === 0) {
                            console.log('API returned 401. Attempting to refresh token...');
                            
                            // Обновляем токен и пробуем снова
                            if (!refreshToken) {
                                throw new Error('Refresh token is missing. Cannot continue.');
                            }
                            const newTokens = await refreshAccessToken(refreshToken); // Обновляет глобальные токены
                            currentAccessToken = newTokens.accessToken; // Используем новый токен
                            retryCount++; // Увеличиваем счетчик попыток
                            continue; // Начать цикл заново с новым токеном
                        } else {
                            // Если и после обновления 401, то ошибка
                            throw new Error('Failed to refresh token or access token is still invalid.');
                        }
                    }

                    if (!response.ok) {
                        // Другие API ошибки (400, 500 и т.д.)
                        const errorData = await response.json();
                        throw new Error(`API Error: ${response.status} - ${errorData.error_description || errorData.error || 'Unknown error'}`);
                    }
                    
                    // Успех, выходим из цикла retry
                    const data = await response.json();
                    success = true; 
                    
                    if (data && data.data && data.data.length > 0) {
                        allContacts = allContacts.concat(data.data);
                        page++;
                    } else {
                        hasMore = false;
                    }

                } catch (error) {
                    // Перехват ошибки обновления или другой критической ошибки
                    console.error('Error fetching page or refreshing token:', error);
                    hasMore = false; // Прерываем внешний цикл пагинации
                    throw error; // Бросаем ошибку в основной catch-блок
                }
            } // конец цикла retry
            
            // Если успех был, но hasMore стал false в теле if (data...), выходим из while
            if (!hasMore) break; 
        } // конец цикла while (пагинация)

        // ... (Остальная логика: фильтрация, агрегация, запись в Google Sheets) ...

        const start = new Date(startDate);
        const end = new Date(endDate);
        startD = start 
        endD = end
      const filteredContacts = allContacts.filter(contact => {
      if (!contact.created_at) return false;
      const createdAt = new Date(contact.created_at);
      return createdAt >= start && createdAt <= end;
    });

    console.log(`Найдено ${filteredContacts.length} контактов в указанный период. ${allSources}`);

    // 4. Преобразуем отфильтрованные данные для Google Sheets
    // const googleSheetsFormattedData = transformListokCRMDataForGoogleSheets(filteredContacts);
    const googleSheetsFormattedData = await aggregateSourcesForGoogleSheets(filteredContacts,allSources,currentAccessToken)

    // 5. Записываем данные в Google Sheets
    const spreadsheetId = '1AMIZaR1ADV_0aVP-m80rklhXUTClsQwVB-GrVMI3YPA';  
    await writeDataToSheet(spreadsheetId, googleSheetsFormattedData);

    try {
    const sheetId = await getSheetId(spreadsheetId);
 
    await formatSheet(spreadsheetId, sheetId); 

} catch (formatError) {
    console.warn('Could not auto-resize columns:', formatError.message);
    // Продолжаем выполнение, так как запись данных прошла успешно
}
        res.json({ message: `Успешно записан отчет по ${filteredContacts.length} контактам в Google Таблицу.` });

    } catch (error) {
        // Ловим все критические ошибки, включая отказ в обновлении токена
        console.error('Final error processing filtered contacts:', error);
        res.status(500).json({ error: 'Internal server error while processing filtered contacts or failed authentication.' });
    }
});

// Функция для преобразования данных ListokCRM для Google Sheets
function transformListokCRMDataForGoogleSheets(data) {
  if (!data || !Array.isArray(data) || data.length === 0) {
    return [];
  }

  const headers = Object.keys(data[0]);
  const googleSheetsData = [headers];

  data.forEach(item => {
    const row = headers.map(header => item[header]);
    googleSheetsData.push(row);
  });

  return googleSheetsData;
}



async function formatSheet(spreadsheetId, sheetId) {
    const sheets = await authorize();
    
    const requests = [];
    
    // --- A. Автоподбор ширины (всего 13 столбцов: A-M) ---
    requests.push({
        autoResizeDimensions: {
            dimensions: {
                sheetId: sheetId,
                dimension: 'COLUMNS',
                startIndex: 0,
                endIndex: 13 
            }
        }
    });

   // --- B. Окрашивание столбцов ручного ввода (B, D) ---
const inputColumns = [2, 7, 10, 12]; // B, D
const yellowColor = { red: 0.98, green: 0.93, blue: 0.8 };

inputColumns.forEach(index => {
    requests.push({
        repeatCell: {
            range: {
                sheetId: sheetId,
                startColumnIndex: index,
                endColumnIndex: index + 1, // Правильное поле
                startRowIndex: 1 // Начинаем со второй строки (после заголовка)
            },
            cell: {
                userEnteredFormat: {
                    backgroundColor: yellowColor,
                }
            },
            fields: 'userEnteredFormat.backgroundColor'
        }
    });
});

// --- C. ФОРМАТИРОВАНИЕ ЧИСЕЛ И ВАЛЮТ ---

// 1. Валюта (E, F, I)
const currencyColumns = [4, 5, 8];
currencyColumns.forEach(index => {
    requests.push({
        repeatCell: {
            range: { 
                sheetId: sheetId, 
                startColumnIndex: index, 
                endColumnIndex: index + 1, // Правильное поле
                startRowIndex: 1 
            },
            cell: {
                userEnteredFormat: {
                    numberFormat: {
                        type: 'CURRENCY',
                        pattern: '#,##0.00 ₽'
                    }
                }
            },
            fields: 'userEnteredFormat.numberFormat'
        }
    });
});

// 2. Процент (G, J, L)
const percentColumns = [6, 9, 11];
percentColumns.forEach(index => {
    requests.push({
        repeatCell: {
            range: { 
                sheetId: sheetId, 
                startColumnIndex: index, 
                endColumnIndex: index + 1, // Правильное поле
                startRowIndex: 1 
            },
            cell: {
                userEnteredFormat: {
                    numberFormat: {
                        type: 'PERCENT',
                        pattern: '0.00%' 
                    }
                }
            },
            fields: 'userEnteredFormat.numberFormat'
        }
    });
});

// 3. Целые числа (C, H, K, M)
const integerColumns = [2, 7, 10, 12];
integerColumns.forEach(index => {
    requests.push({
        repeatCell: {
            range: { 
                sheetId: sheetId, 
                startColumnIndex: index, 
                endColumnIndex: index + 1, // Правильное поле
                startRowIndex: 1 
            },
            cell: {
                userEnteredFormat: {
                    numberFormat: {
                        type: 'NUMBER',
                        pattern: '0' 
                    }
                }
            },
            fields: 'userEnteredFormat.numberFormat'
        }
    });
});

    const request = {
        spreadsheetId: spreadsheetId,
        resource: { requests: requests }
    };

    try {
        await sheets.spreadsheets.batchUpdate(request);
        console.log('Sheet successfully formatted (widths, colors, number types).');
    } catch (err) {
        console.error('Error during batchUpdate (formatting):', err);
        throw err;
    }
}
async function getSheetId(spreadsheetId) {
    const sheets = await authorize();
    const response = await sheets.spreadsheets.get({
        spreadsheetId: spreadsheetId,
        fields: 'sheets.properties'
    });
    
    // Возвращаем ID первого листа
    if (response.data.sheets && response.data.sheets.length > 0) {
        return response.data.sheets[0].properties.sheetId;
    }
    throw new Error('No sheets found in the spreadsheet.');
}

async function getContactListings(accessToken, contactId, maxRetries = 3) {
    if (!contactId) {
        throw new Error("Contact ID must be provided.");
    }
    
    // Формируем URL: /api/external/v2/contacts/{contactId}/listings
    const baseURL = 'https://an8242.listokcrm.ru/api/external/v2/contacts'
    const url = `${baseURL}/${contactId}/listings`;

    const headers = {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    };

    // --- ЛОГИКА ПОВТОРНЫХ ПОПЫТОК ---
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const response = await fetch(url, {
                method: "GET",
                headers: headers,
            });

            if (response.status === 429) {
                // Если сработал Rate Limiter и это не последняя попытка
                if (attempt < maxRetries - 1) {
                    const errorText = await response.text();
                    
                    // Расчет задержки: 1с, 2с, 4с + случайный шум
                    const delay = Math.pow(2, attempt) * 1000 + (Math.random() * 500); 
                    
                    console.warn(`429 Rate Limit hit for contact ${contactId} (Attempt ${attempt + 1}). Retrying in ${Math.round(delay/1000)}s.`);
                    await sleep(delay);
                    
                    continue; // Переходим к следующей попытке
                } else {
                    // Если это последняя попытка и снова 429
                    const errorText = await response.text();
                    throw new Error(`API Rate Limit (429) hit for contact ${contactId} listings: ${errorText}`);
                }
            }

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Failed to fetch listings for contact ${contactId}. Status: ${response.status}. Response: ${errorText}`);
            }

            const data = await response.json();
            
            // Успешный выход из цикла и возврат данных
            if (data && Array.isArray(data.data)) {
                return data.data; 
            }
            return data; 

        } catch (error) {
            // Если произошла сетевая ошибка или другая ошибка, не связанная с 429,
            // и это не последняя попытка, можно попробовать снова после короткой паузы.
            if (attempt === maxRetries - 1 || error.message.includes('API Rate Limit')) {
                throw error;
            }
            console.warn(`Temporary error on attempt ${attempt + 1}: ${error.message}. Retrying...`);
            await sleep(500); // Небольшая пауза при сетевой ошибке
            continue;
        }
    }
    
    // Этот код должен быть недостижим, если maxRetries > 0, но для безопасности:
    throw new Error(`Failed to fetch listings for contact ${contactId} after ${maxRetries} attempts.`);
}

async function getContactSpecificPasses(accessToken, contactId, maxRetries = 3) {
    if (!contactId ) {
        throw new Error("Contact ID and Pass Contact ID must be provided.");
    }
    
    // Формируем URL: /api/external/v2/contacts/{contactId}/passes/{passContactId}
    const baseURL = 'https://an8242.listokcrm.ru/api/external/v2'
    const url = `${baseURL}/contacts/${contactId}/passes`;

    const headers = {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    };

    // --- ЛОГИКА ПОВТОРНЫХ ПОПЫТОК ---
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const response = await fetch(url, {
                method: "GET",
                headers: headers,
            });

            if (response.status === 429) {
                if (attempt < maxRetries - 1) {
                    const delay = Math.pow(2, attempt) * 1000 + (Math.random() * 500); 
                    
                    console.warn(`429 Rate Limit hit for contact ${contactId} (Attempt ${attempt + 1}). Retrying in ${Math.round(delay/1000)}s.`);
                    await sleep(delay);
                    
                    continue; // Переходим к следующей попытке
                } else {
                    const errorText = await response.text();
                    throw new Error(`API Rate Limit (429) hit after ${maxRetries}  ${errorText}`);
                }
            }

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Failed to fetch for contact ${contactId}. Status: ${response.status}. Response: ${errorText}`);
            }

            const data = await response.json();
            
             if (data && Array.isArray(data.data)) {
                return data.data; 
            }
            return data; 

        } catch (error) {
            if (attempt === maxRetries - 1 || error.message.includes('API Rate Limit')) {
                throw error;
            }
            console.warn(`Temporary error on attempt ${attempt + 1}. Retrying...`);
            await sleep(500); 
            continue;
        }
    }
    
    throw new Error(`Failed to fetch after ${maxRetries} attempts.`);
}
 

async function getEventById(accessToken, eventId, maxRetries = 3) {
    if (!eventId) {
        throw new Error("Event ID must be provided.");
    }
    
    const baseURL = 'https://an8242.listokcrm.ru/api/external/v2';
    const url = `${baseURL}/events/${eventId}`;

    const headers = {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    };

    // --- ЛОГИКА ПОВТОРНЫХ ПОПЫТОК ---
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const response = await fetch(url, {
                method: "GET",
                headers: headers,
            });

            if (response.status === 429) {
                // Если сработал Rate Limiter и это не последняя попытка
                if (attempt < maxRetries - 1) {
                    const errorText = await response.text();
                    
                    // Расчет задержки: 1с, 2с, 4с + случайный шум
                    const delay = Math.pow(2, attempt) * 1000 + (Math.random() * 500); 
                    
                    console.warn(`429 Rate Limit hit for event ${eventId} (Attempt ${attempt + 1}). Retrying in ${Math.round(delay/1000)}s.`);
                    await sleep(delay);
                    
                    continue; // Переходим к следующей попытке
                } else {
                    // Если это последняя попытка и снова 429
                    const errorText = await response.text();
                    throw new Error(`API Rate Limit (429) hit for event ${eventId}: ${errorText}`);
                }
            }

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Failed to fetch event ${eventId}. Status: ${response.status}. Response: ${errorText}`);
            }

            const data = await response.json();
            
            // Возвращаем данные события
            return data;

        } catch (error) {
            // Если произошла сетевая ошибка или другая ошибка, не связанная с 429,
            // и это не последняя попытка, можно попробовать снова после короткой паузы.
            if (attempt === maxRetries - 1 || error.message.includes('API Rate Limit')) {
                throw error;
            }
            console.warn(`Temporary error on attempt ${attempt + 1}: ${error.message}. Retrying...`);
            await sleep(500); // Небольшая пауза при сетевой ошибке
            continue;
        }
    }
    
    // Этот код должен быть недостижим, если maxRetries > 0, но для безопасности:
    throw new Error(`Failed to fetch event ${eventId} after ${maxRetries} attempts.`);
}
 
async function getContactAdmissions(accessToken, contactId, maxRetries = 3) {
    if (!contactId) {
        throw new Error("Contact ID must be provided.");
    }
    
    const baseURL = 'https://an8242.listokcrm.ru/api/external/v2';
    const url = `${baseURL}/contacts/${contactId}/admissions`;

    const headers = {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    };

    // --- ЛОГИКА ПОВТОРНЫХ ПОПЫТОК ---
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const response = await fetch(url, {
                method: "GET",
                headers: headers,
            });

            if (response.status === 429) {
                // Если сработал Rate Limiter и это не последняя попытка
                if (attempt < maxRetries - 1) {
                    const errorText = await response.text();
                    
                    // Расчет задержки: 1с, 2с, 4с + случайный шум
                    const delay = Math.pow(2, attempt) * 1000 + (Math.random() * 500); 
                    
                    console.warn(`429 Rate Limit hit for contact ${contactId} admissions (Attempt ${attempt + 1}). Retrying in ${Math.round(delay/1000)}s.`);
                    await sleep(delay);
                    
                    continue; // Переходим к следующей попытке
                } else {
                    // Если это последняя попытка и снова 429
                    const errorText = await response.text();
                    throw new Error(`API Rate Limit (429) hit for contact ${contactId} admissions: ${errorText}`);
                }
            }

            if (response.status === 404) {
                // Событие не существует, нет смысла пробовать снова
                const errorText = await response.text();
                throw new Error(`Contact with ID ${contactId} admissions not found: ${errorText}`);
            }

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Failed to fetch admissions for contact ${contactId}. Status: ${response.status}. Response: ${errorText}`);
            }

            const data = await response.json();
            
            // Успешный выход из цикла и возврат данных
            if (data && Array.isArray(data.data)) {
                return data.data; 
            }
            return data; 

        } catch (error) {
            // Если произошла сетевая ошибка или другая ошибка, не связанная с 429 или 404,
            // и это не последняя попытка, можно попробовать снова после короткой паузы.
            if (attempt === maxRetries - 1 || error.message.includes('API Rate Limit') || error.message.includes('not found')) {
                throw error;
            }
            console.warn(`Temporary error on attempt ${attempt + 1}: ${error.message}. Retrying...`);
            await sleep(500); // Небольшая пауза при сетевой ошибке
            continue;
        }
    }
    
    // Этот код должен быть недостижим, если maxRetries > 0, но для безопасности:
    throw new Error(`Failed to fetch admissions for contact ${contactId} after ${maxRetries} attempts.`);
}

function getPassNameFromPass(contactPass){
  const id = contactPass.pass_id
     for(const pass of allPasses || []){
      if(pass.pass_id === id){
        return pass.name
      } 
    }
             return null

}

function convertDateTimeToDate(dateTimeString) {
    if (!dateTimeString) {
        return null;
    }
    
    const date = new Date(dateTimeString);
    return date.toISOString().split('T')[0]; // Берем только дату до "T"
}

function hasValidPass(pases, excludedPassesNames) {
    if (!pases || !Array.isArray(pases) || !excludedPassesNames || !Array.isArray(excludedPassesNames)) {
        return false;
    }
    
    for (const pass of pases) {
        // Проверяем, что у абонемента есть вложенный объект pass с name
        const passName = pass.pass?.name;
        if (passName && !excludedPassesNames.includes(passName)) {
            return true; // Нашли хотя бы один подходящий абонемент
        }
    }
    
    return false; // Ни один абонемент не прошел проверку
}


function hasValidPassWithData(pases, excludedPassesNames, targetDate) {
    if (!pases || !Array.isArray(pases) || !excludedPassesNames || !Array.isArray(excludedPassesNames)) {
        return false;
    }
    
    // Преобразуем целевую дату в объект Date для сравнения
    const targetDateTime = new Date(targetDate).getTime();
    if (isNaN(targetDateTime)) {
        throw new Error(`Invalid target date: ${targetDate}`);
    }
    
    for (const pass of pases) {
        // Проверяем, что у абонемента есть вложенный объект pass с name
        const passName = pass.pass?.name;
        
        // Проверяем дату оформления (sold_at в секундах) или created_at (строка)
        let passDate = null;
        if (pass.sold_at) {
            // Если есть sold_at (в секундах), преобразуем в миллисекунды
            passDate = new Date(pass.sold_at * 1000).getTime();
        } else if (pass.created_at) {
            // Если нет sold_at, используем created_at (строка даты)
            passDate = new Date(pass.created_at).getTime();
        }
        
        // Проверяем, что дата существует и меньше целевой даты
        if (passDate && passDate < targetDateTime) {
            // Проверяем, что имя абонемента не входит в исключения
            if (passName && !excludedPassesNames.includes(passName)) {
                return true; // Нашли хотя бы один подходящий абонемент
            }
        }
    }
    
    return false; // Ни один абонемент не прошел проверку
}

async function aggregateSourcesForGoogleSheets(contacts, allSources, accessToken) {
   const titles = ['Источник', 'Из Кабинета', 'Лиды', 'Бюджет', 'С НДС', 'Цена Лида С НДС', 'Конверсия', 'Записи Всего', 'Цена записи', '%CV пробное/зап', 'Пробное всего', '%CV покупка/пробное', 'Покупка всего', '',	'цена клиента'	,'Записи до конца мес',	'Пробников до конца мес',	'Покупок до конца мес',	'посещений',	'посещений на ученика',	'cv зап/лид до конца мес',	'cv2 проб/зап до конца мес',	'cv покупка/проб до конца мес',	'cv покупка/лид до конца мес']
    if (!contacts || contacts.length === 0 || !allSources || allSources.length === 0) {
        return [
          titles
        ];
    }

     // 1. Создаем карты для подсчета
    const sourceCounts = {};          
    const totalRecordsCounts = {};    
    const totalAdmissionsCount = {};  
    const totalBuys = {};             
    const totalMounthRecordsCounts = {}; 
    const totalMounthAdmissionsCount = {}   
    const totalMounthBuys = {}        
    
    // --- 1.1. ДОБАВЛЯЕМ ВИРТУАЛЬНЫЙ ИСТОЧНИК "БЕЗ ИСТОЧНИКА" ---
    const aggregatedSources = [...allSources]; // Создаем копию
    
    // Проверяем, существует ли уже источник с ID 0 (хотя это маловероятно)
    const noSourceId = '0';
    if (!aggregatedSources.some(source => String(source.id) === noSourceId)) {
         aggregatedSources.push({ id: 0, name: 'Без источника' });
    }

    // Инициализация счетчиков: используем aggregatedSources
    aggregatedSources.forEach(source => {
        const idKey = String(source.id); 
        
        sourceCounts[idKey] = 0;
        totalRecordsCounts[idKey] = 0;
        totalAdmissionsCount[idKey] = 0;
        totalBuys[idKey] = 0;
        totalMounthRecordsCounts[idKey] = 0;
        totalMounthAdmissionsCount[idKey] = 0;
        totalMounthBuys[idKey] = 0
    });

    // ----------------------------------------------------
    // 2. АГРЕГАЦИЯ: ИСПОЛЬЗУЕМ P-LIMIT
    // ----------------------------------------------------
    
    const contactPromises = contacts.map(contact => 
        limit(async () => {
            // Если source_id === 0, он будет преобразован в строковый ключ '0', 
            // который мы добавили в aggregatedSources.
            const sourceIdKey = String(contact.source_id); 
            
            // Если source_id null/undefined/пусто, используем '0'
            const finalSourceIdKey = (sourceIdKey === '0' || !sourceIdKey) 
                                     ? '0' 
                                     : sourceIdKey;
            
            if (finalSourceIdKey && sourceCounts.hasOwnProperty(finalSourceIdKey)) {
                
                // Подсчет Лидов (C)
                sourceCounts[finalSourceIdKey]++;
                
                // --- АСИНХРОННЫЙ ЗАПРОС LISTINGS ---
                let listings = [];
                let admissions = []
                try {
                    listings = await getContactListings(accessToken, contact.id || contact.contact_id); 
                    
                    if( listings && listings.length > 0){
                       totalRecordsCounts[finalSourceIdKey]++; 
                       const ev = await getEventById(accessToken,listings[0].event_id)
                       if(ev){
                        const evDate = new Date(ev.date);
                        if (evDate <= endD){
                          totalMounthRecordsCounts[finalSourceIdKey]++
                          // console.log(evDate,'  <  ', endD)
                        }
                      }
                      admissions = await getContactAdmissions(accessToken, contact.id || contact.contact_id)
                      if(admissions && admissions.length > 0){
                          totalAdmissionsCount[finalSourceIdKey]++;
                          const date = new Date(convertDateTimeToDate(admissions[0].created_at))
                           if (date <= endD){
                          totalMounthAdmissionsCount[finalSourceIdKey]++
                          }
                           const passes = await getContactSpecificPasses(accessToken, contact.id || contact.contact_id)
                           if(passes && passes.length > 0){
                             if(hasValidPass(passes,excludedPassesNames)){
                               totalBuys[finalSourceIdKey]++;
                              }
                              if(hasValidPassWithData(passes,excludedPassesNames,endD)){
                               totalMounthBuys[finalSourceIdKey]++
                              }
                            }
                            // const passName = getPassNameFromPass(passes[0])
                            // if (passName && !excludedPassesNames.includes(passName)){
                      }
                      }
                    // if (listings && listings.length > 0 && contact.last_admission_date && contact.last_admission_date !== 0) {
                    //     totalAdmissionsCount[finalSourceIdKey]++;
                    //     const ev = await getEventById(accessToken,contact.last_admission_date)
                    //    if(ev){
                    //     const evDate = new Date(ev.date);
                    //     if (evDate <= endD){
                    //       totalMounthAdmissionsCount[finalSourceIdKey]++
                    //       // console.log(evDate,'  <  ', endD)
                    //     }
                    //   }
                    // }
                    // if (listings && listings.length > 0 && contact.last_admission_date && contact.last_admission_date !== 0 && contact.last_pass_purchased && contact.last_pass_purchased !== 0 ) {
                    //   const pass = await getContactSpecificPass(accessToken, contact.id || contact.contact_id, contact.last_pass_purchased)
                    //   const passName = getPassNameFromPass(pass)
                    //   if (passName && !excludedPassesNames.includes(passName)){
                    //     totalBuys[finalSourceIdKey]++;
                    //   }
                    // }
                } catch (error) {
                    console.warn(`Skipping listings for contact ${contact.id}: ${error.message}`);
                }
                
            }
        })
    );
    
    // 3. ЖДЕМ ЗАВЕРШЕНИЯ ВСЕХ ЗАДАЧ
    try {
        await Promise.all(contactPromises);
        console.log("Parallel aggregation completed.");
    } catch (e) {
        console.error("Critical error during parallel processing:", e.message);
    }
     
    // 4. ФОРМАТИРОВАНИЕ РЕЗУЛЬТАТА: Используем aggregatedSources
    
    const googleSheetsData = [
       titles
      ];
    
    let currentRow = 2; 

    // ИТЕРИРУЕМСЯ ПО РАСШИРЕННОМУ СПИСКУ ИСТОЧНИКОВ
    aggregatedSources.forEach(source => { 
        // ... (Остальная логика форматирования остается прежней) ...
        const sourceIdKey = String(source.id);
        const countLeads = sourceCounts[sourceIdKey];

        // --- ФИЛЬТРАЦИЯ: Удаляем строки, где Лидов <= 0 ---
        if (countLeads <= 0) {
             return; 
        }
        
        const countRecords = totalRecordsCounts[sourceIdKey];
        const countAdmissions = totalAdmissionsCount[sourceIdKey]
        const totalBuy = totalBuys[sourceIdKey]
        const mounthRecords = totalMounthRecordsCounts[sourceIdKey]
        const mounthAdmissions = totalMounthAdmissionsCount[sourceIdKey]
        const mounthBuys = totalMounthBuys[sourceIdKey]
        const rowNum = currentRow++; 
        
        // --- ФОРМУЛЫ ---
         
        const formulaE = `=ЕСЛИ(ЕПУСТО(D${rowNum}); 0; D${rowNum}*1,2)`;
        const formulaF = `=ЕСЛИ(C${rowNum}=0; 0; E${rowNum}/C${rowNum})`;
        const formulaG = `=ЕСЛИ(C${rowNum}=0; 0; H${rowNum}/C${rowNum})`;
        const formulaI = `=ЕСЛИ(H${rowNum}=0; 0; E${rowNum}/H${rowNum})`;
        const formulaJ = `=ЕСЛИ(H${rowNum}=0; 0; K${rowNum}/H${rowNum})`;
        const formulaL = `=ЕСЛИ(K${rowNum}=0; 0; M${rowNum}/K${rowNum})`;
        const formulaN = '';
        const formulaO = `=ЕСЛИ(C${rowNum}=0; 0; E${rowNum}/M${rowNum})`; 

        
        const row = [
            source.name, 
            '', 
            countLeads, 
            '',
            formulaE, 
            formulaF, 
            formulaG,
            countRecords,
            formulaI,
            formulaJ,
            countAdmissions,
            formulaL,
            totalBuy,
            formulaN,
            formulaO,
            mounthRecords, mounthAdmissions, mounthBuys, '', '', '', '', '', 
        ];
        
        googleSheetsData.push(row);
    });

    return googleSheetsData;
}

app.post('/refresh_token', async (req, res) => {
  const { refreshToken: receivedRefreshToken } = req.body;

  const client_id = process.env.LISTOKCRM_CLIENT_ID;
  const client_secret = process.env.LISTOKCRM_CLIENT_SECRET;

  if (!receivedRefreshToken) {
    return res.status(400).json({ error: 'Refresh token is required' });
  }

  try {
    const response = await fetch('https://an8242.listokcrm.ru/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token: receivedRefreshToken,
        client_id: client_id,
        client_secret: client_secret,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      console.error('ListokCRM error during refresh token:', data);
      return res.status(response.status).json({ error: data.error_description || data.error || 'Unknown error' });
    }

    accessToken = data.access_token;
    refreshToken = data.refresh_token;
    console.log('Access token refreshed:', accessToken);
    res.json(data);
  } catch (error) {
    console.error('Error during refresh token:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Функция для обновления токена доступа
async function refreshAccessToken(refreshToken) {
  const client_id = process.env.LISTOKCRM_CLIENT_ID;
  const client_secret = process.env.LISTOKCRM_CLIENT_SECRET;

  try {
    const response = await fetch('https://an8242.listokcrm.ru/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token: refreshToken,
        client_id: client_id,
        client_secret: client_secret,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      console.error('ListokCRM error during refresh token:', data);
      throw new Error(data.error_description || data.error || 'Failed to refresh token');
    }

    accessToken = data.access_token;
    refreshToken = data.refresh_token;
    console.log('Access token refreshed:', accessToken);
    return { accessToken, refreshToken };

  } catch (error) {
    console.error('Error during refresh token:', error);
    throw error;
  }
}

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});






// 4:50  5:10