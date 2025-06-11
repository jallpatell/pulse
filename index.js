import puppeteer from 'puppeteer';
import axios from 'axios';
import { google } from 'googleapis';
import dotenv from 'dotenv';

dotenv.config();

const CONFIG = {
    groqApiKey: process.env.GROQ_API_KEY || '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',
    websiteUrl: 'https://pulse.zerodha.com',
    groqModel: 'deepseek-r1-distill-llama-70b',
    apiInterval: process.env.API_INTERVAL ? parseInt(process.env.API_INTERVAL) : 30000,
    groqRequestDelay: process.env.GROQ_REQUEST_DELAY ? parseInt(process.env.GROQ_REQUEST_DELAY) : 1000,
    maxHtmlContentLength: process.env.MAX_HTML_CONTENT_LENGTH ? parseInt(process.env.MAX_HTML_CONTENT_LENGTH) : 1000,
    wpApiUrl: 'https://profitbooking.in/wp-json/scraper/v1/zerodha',
    wpUser: process.env.WP_USER || 'your_wp_username',
    wpPass: process.env.WP_PASS || 'your_wp_password',
    googleSheet: {
        sheetId: process.env.SHEET_ID || '1LK8uo_pDJkMUKtF2-MwpCo4nyh_EHzcFuV0_UuN6LXQ',
        sheetName: process.env.SHEET_NAME || 'Sheet1',
        serviceAccount: {
            email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || 'your-service-account@project.iam.gserviceaccount.com',
            privateKey: process.env.GOOGLE_PRIVATE_KEY ?
                process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') :
                `-----BEGIN PRIVATE KEY-----
        YourPrivateKeyHere
        -----END PRIVATE KEY-----`
        }
    }
};

class NewsProcessor {
    constructor() {
        this.queue = [];
        this.isProcessing = false;
        this.validNSC = new Set();
        this.failedAttempts = 0;
        this.maxRetries = 5;
    }

    async initialize() {
        await this.loadSymbolsFromSheet();
        console.log('NewsProcessor initialized');
    }

    async loadSymbolsFromSheet() {
        try {
            console.log('Loading symbols from Google Sheet...');

            const privateKey = CONFIG.googleSheet.serviceAccount.privateKey;
            if (!privateKey || !privateKey.startsWith('-----BEGIN PRIVATE KEY-----')) {
                throw new Error('Invalid private key format. Must start with "-----BEGIN PRIVATE KEY-----"');
            }

            const formattedKey = privateKey.replace(/\\n/g, '\n');

            const auth = new google.auth.JWT({
                email: CONFIG.googleSheet.serviceAccount.email,
                key: formattedKey,
                scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
            });

            await auth.authorize();
            console.log('Successfully authenticated with Google Sheets API');

            const sheets = google.sheets({ version: 'v4', auth });

            const response = await sheets.spreadsheets.values.get({
                spreadsheetId: CONFIG.googleSheet.sheetId,
                range: `${CONFIG.googleSheet.sheetName}!A:C`,
            });

            const rows = response.data.values;
            if (!rows || rows.length < 2) {
                throw new Error('No data found in sheet or only header row is present.');
            }

            const headers = rows[0];
            const symbolCol = headers.findIndex(header => header.toLowerCase() === 'symbol');
            if (symbolCol === -1) throw new Error('Symbol column not found in Google Sheet. Make sure there is a header named "Symbol"');

            const symbols = rows.slice(1)
                .map(row => row[symbolCol]?.toString().toUpperCase().trim())
                .filter(s => s && s.length > 0);

            this.validNSC = new Set(symbols);
            console.log(`Loaded ${this.validNSC.size} valid symbols from Google Sheet.`);
            this.failedAttempts = 0;
        } catch (err) {
            this.failedAttempts++;

            if (err.message.includes('DECODER routines') || err.message.includes('PEM')) {
                console.error('Private key format error: Please ensure your GOOGLE_PRIVATE_KEY is properly formatted with \\n (double backslash n) for line breaks in your .env file, or direct line breaks if hardcoded.');
                console.error('Example format in .env: GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\\nYOUR_KEY_PART1\\nYOUR_KEY_PART2\\n-----END PRIVATE KEY-----"');
            } else {
                console.error(`Failed to load symbols (attempt ${this.failedAttempts} of ${this.maxRetries}):`, err.message);
            }

            if (this.failedAttempts >= this.maxRetries) {
                console.error('Max retry attempts reached for loading symbols. Please check your Google Sheet configuration and network connection.');
                throw err;
            }

            const delay = Math.min(60000, 1000 * Math.pow(2, this.failedAttempts));
            console.log(`Retrying in ${delay / 1000} seconds...`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return this.loadSymbolsFromSheet();
        }
    }

    async addToQueue(items) {
        if (!Array.isArray(items)) {
            items = [items];
        }

        const newItems = items.filter(item =>
            item &&
            item.htmlContent &&
            !this.queue.some(existing =>
                existing.htmlContent === item.htmlContent
            )
        );

        if (newItems.length > 0) {
            this.queue.push(...newItems);
            console.log(`Added ${newItems.length} new items to queue (Total: ${this.queue.length})`);

            if (!this.isProcessing) {
                this.processQueue();
            }
        }
    }

    async processQueue() {
        if (this.isProcessing || this.queue.length === 0) return;

        this.isProcessing = true;
        const article = this.queue.shift();

        try {
            console.log(`Processing HTML of: "${article.htmlContent.substring(0, 50).replace(/\s+/g, ' ').trim()}..."`);

            const result = await this.analyzeWithAI(article);
            if (result) {
                await this.storeInWordPress(result);
            } else {
                console.log('No relevant company found by AI, or AI response was invalid. Skipping article.');
            }
        } catch (err) {
            console.error('Processing error:', err.message);
        } finally {
            this.isProcessing = false;

            if (this.queue.length > 0) {
                setImmediate(() => this.processQueue());
            }
        }
    }

    async analyzeWithAI(article) {
        const htmlContent = article.htmlContent.substring(0, CONFIG.maxHtmlContentLength);

        const companyList = Array.from(this.validNSC).join(', ');

        const prompt = `
            From HTML, identify relevant NSE company (from ${companyList}).
            Extract: "company_name", "headline" (.title a), "description" (.desc), "nsc" (uppercase symbol), "source" (.feed, remove '— '), "url" (.title a href).
            If no relevant company, set "nsc" and all other fields to null.
            Output JSON: {"company_name":"string|null", "headline":"string|null", "description":"string|null", "nsc":"string|null", "source":"string|null", "url":"string|null"}.
            HTML: ${htmlContent}`;

        for (let attempt = 1; attempt <= 3; attempt++) {
            try {
                await new Promise(res => setTimeout(res, CONFIG.groqRequestDelay));

                const response = await axios.post(
                    'https://api.groq.com/openai/v1/chat/completions',
                    {
                        model: CONFIG.groqModel,
                        messages: [{
                            role: "user",
                            content: prompt
                        }],
                        temperature: 0.2,
                        response_format: { type: "json_object" },
                        max_tokens: 1000
                    },
                    {
                        headers: {
                            'Authorization': `Bearer ${CONFIG.groqApiKey}`,
                            'Content-Type': 'application/json'
                        },
                        timeout: 30000
                    }
                );

                return this.parseAIResponse(response);
            } catch (err) {
                const errorMessage = err.response?.data?.error?.message || err.message;
                console.error(`AI Attempt ${attempt} failed:`, errorMessage);

                if (err.response?.status === 429 || (err.response?.data?.error?.code === 'rate_limit_exceeded')) {
                    let retryAfterMs = 5000;
                    const match = errorMessage.match(/try again in (\d+\.?\d*)s/);
                    if (match && parseFloat(match[1])) {
                        retryAfterMs = parseFloat(match[1]) * 1000 + 500;
                        console.log(`Groq rate limit: Waiting for ${retryAfterMs / 1000}s before retrying.`);
                    } else {
                        console.log(`Groq rate limit: Using default wait of ${retryAfterMs / 1000}s before retrying.`);
                    }
                    if (attempt < 3) {
                        await new Promise(res => setTimeout(res, retryAfterMs));
                        continue;
                    }
                }

                if (attempt < 3) await new Promise(res => setTimeout(res, 5000));
            }
        }
        return null;
    }

    cleanseText(text) {
        return text
            .replace(/[^\w\s.,-]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .substring(0, 3000);
    }

    parseAIResponse(response) {
        try {
            if (!response?.data?.choices?.[0]?.message?.content) {
                console.log('AI response content is empty.');
                return null;
            }

            const content = response.data.choices[0].message.content;
            let result;
            try {
                result = JSON.parse(content);
            } catch (jsonErr) {
                console.error('Failed to parse AI response JSON:', jsonErr.message, 'Raw content:', content);
                return null;
            }

            if (!result || typeof result.nsc !== 'string' || !result.nsc.trim()) {
                console.log('AI did not return a valid NSC symbol or returned NULL as requested.');
                return null;
            }

            const nsc = result.nsc.toUpperCase().trim();
            if (!this.validNSC.has(nsc)) {
                console.log(`Unrecognized NSC symbol "${nsc}" returned by AI. This symbol is not in your valid list.`);
                return null;
            }

            return {
                company_name: typeof result.company_name === 'string' ? result.company_name.trim() : 'Unknown Company',
                headline: typeof result.headline === 'string' ? result.headline.trim() : 'No Title',
                description: typeof result.description === 'string' ? result.description.trim() : '',
                nsc: nsc,
                news_date: this.formatDate(),
                source: typeof result.source === 'string' ? result.source.trim() : 'Unknown',
                url: typeof result.url === 'string' ? result.url.trim() : null
            };
        } catch (err) {
            console.error('Error during AI response parsing and validation:', err);
            return null;
        }
    }

    formatDate(dateString) {
        try {
            const date = dateString ? new Date(dateString) : new Date();
            if (isNaN(date.getTime())) {
                console.warn(`Invalid date string received for formatting: ${dateString}. Using current date.`);
                return new Date().toISOString().split('T')[0];
            }
            return date.toISOString().split('T')[0];
        } catch (e) {
            console.error('Error in formatDate:', e);
            return new Date().toISOString().split('T')[0];
        }
    }

    async storeInWordPress(data) {
        try {
            const requiredFields = ['company_name', 'headline', 'nsc'];
            if (!requiredFields.every(field => data[field])) {
                console.log('Skipping WordPress storage: Missing required fields (company_name, headline, nsc). Data:', data);
                return;
            }

            const payload = {
                title: data.headline,
                description: data.description,
                company: data.company_name,
                nsc: data.nsc,
                news_date: data.news_date,
                source: data.source,
                url: data.url
            };

            const response = await axios.post(CONFIG.wpApiUrl, payload, {
                auth: {
                    username: CONFIG.wpUser,
                    password: CONFIG.wpPass
                },
                timeout: 30000
            });

            if (response.data?.id) {
                console.log(`Stored in WordPress: ${data.nsc} - "${data.headline.substring(0, 30)}..." (Post ID: ${response.data.id})`);
            } else {
                console.log('WordPress server rejected the data or returned an unexpected response:', response.data);
            }
        } catch (error) {
            console.error('WordPress storage failed:', error.response?.data?.message || error.message);
            if (error.response?.status === 401) {
                console.error('Authentication failed for WordPress API. Check WP_USER and WP_PASS in your .env file.');
            } else if (error.response?.status === 404) {
                console.error('WordPress API endpoint not found. Check WP_API_URL in your .env file.');
            }
        }
    }
}


let browserInstance, pageInstance;

async function setupBrowser() {
    if (browserInstance && pageInstance) return;

    console.log('Launching browser...');
    browserInstance = await puppeteer.launch({
        headless: 'new',
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-gpu',
            '--disable-dev-shm-usage',
            '--disable-notifications'
        ],
        timeout: 60000
    });

    pageInstance = await browserInstance.newPage();
    await pageInstance.setViewport({ width: 1280, height: 800 });
    await pageInstance.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    pageInstance.setDefaultNavigationTimeout(60000);
    console.log('Browser setup complete.');
}

async function closeBrowser() {
    if (browserInstance) {
        await browserInstance.close();
        browserInstance = null;
        pageInstance = null;
        console.log('Browser closed.');
    }
}

async function scrapeNews() {
    try {
        if (!pageInstance) {
            await setupBrowser();
        }

        console.log(`Navigating to ${CONFIG.websiteUrl}...`);
        await pageInstance.goto(CONFIG.websiteUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });

        console.log('Waiting for news items to load (max 30 seconds)...');
        await pageInstance.waitForSelector('.box.item', { timeout: 30000 });

        console.log('Extracting news items...');
        const newsItems = await pageInstance.evaluate(() => {
            const items = [];
            document.querySelectorAll('.box.item').forEach(item => {
                const htmlContent = item.outerHTML;

                const titleLinkEl = item.querySelector('.title a');
                if (htmlContent && titleLinkEl) {
                    items.push({
                        htmlContent: htmlContent
                    });
                }
            });
            return items;
        });

        console.log(`Found ${newsItems.length} news items.`);
        return newsItems;
    } catch (err) {
        if (err.name === 'TimeoutError') {
            console.error('Scraping failed: News items did not load within the allowed time.');
            console.error('Possible reasons:');
            console.error('1. Website structure changed - please manually inspect `https://pulse.zerodha.com` to verify selectors (`.box.item`, `.title a`).');
            console.error('2. Slow network connection or website issues - try increasing the `waitForSelector` timeout.');
            console.error('3. Anti-bot measures - consider adjusting user-agent or adding more delays.');
        } else {
            console.error('Scraping failed:', err.message);
        }
        return [];
    }
}

async function main() {
    try {
        if (!CONFIG.groqApiKey || CONFIG.groqApiKey === 'your_groq_api_key') {
            throw new Error('Missing or default GROQ_API_KEY. Please set it in your .env file.');
        }
        if (!CONFIG.googleSheet.serviceAccount.email || CONFIG.googleSheet.serviceAccount.email === 'your-service-account@project.iam.gserviceaccount.com' ||
            !CONFIG.googleSheet.serviceAccount.privateKey || CONFIG.googleSheet.serviceAccount.privateKey.includes('YourPrivateKeyHere')) {
            throw new Error('Missing or default Google Sheet service account credentials. Please configure GOOGLE_SERVICE_ACCOUNT_EMAIL and GOOGLE_PRIVATE_KEY in your .env file.');
        }
        if (!CONFIG.wpUser || CONFIG.wpUser === 'your_wp_username' ||
            !CONFIG.wpPass || CONFIG.wpPass === 'your_wp_password') {
            console.warn('WordPress username or password not configured or using default. WordPress storage may fail.');
        }


        console.log('Starting news processing system...');
        const processor = new NewsProcessor();
        await processor.initialize();

        await setupBrowser();

        const initialNewsItems = await scrapeNews();
        await processor.addToQueue(initialNewsItems);

        setInterval(async () => {
            console.log('---');
            console.log('Running periodic scrape...');
            const newItems = await scrapeNews();
            await processor.addToQueue(newItems);
        }, CONFIG.apiInterval);

        setInterval(async () => {
            console.log('---');
            console.log('Refreshing symbol list from Google Sheet...');
            await processor.loadSymbolsFromSheet();
        }, 6 * 60 * 60 * 1000);

    } catch (err) {
        console.error('Fatal error:', err.message);
        await closeBrowser();
        process.exit(1);
    }
}

process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT. Shutting down...');
    await closeBrowser();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\nReceived SIGTERM. Shutting down...');
    await closeBrowser();
    process.exit(0);
});

main();
