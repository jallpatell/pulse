import puppeteer, { Browser, Page } from 'puppeteer';
import axios, { AxiosError, AxiosResponse } from 'axios';
import { google, sheets_v4 } from 'googleapis';
import dotenv from 'dotenv';
import { JSDOM } from 'jsdom';
import { setTimeout as sleep } from 'timers/promises';

dotenv.config();

// ========== Type Definitions ==========
interface Config {
    groqApiKey: string;
    websiteUrl: string;
    groqModel: string;
    apiInterval: number;
    groqRequestDelay: number;
    maxHtmlContentLength: number;
    wpApiUrl: string;
    wpExtraApiUrl: string;
    wpUser?: string;
    wpPass?: string;
    googleSheet: {
        sheetId?: string;
        sheetName: string;
        serviceAccount: {
            email?: string;
            privateKey?: string;
        };
    };
    groqRateLimit: {
        maxRequests: number;
        intervalMs: number;
        retryAfterMultiplier: number;
    };
    maxConsecutiveErrors: number;
    maxRetries: number;
    backoffBase: number;
    maxBackoff: number;
}

interface ScrapedItem {
    htmlContent: string;
}

interface AIResponse {
    company_name?: string;
    Headline?: string;
    Body?: string;
    nsc?: string | null;
    source?: string;
    url?: string;
}

interface ProcessedResult {
    company_name: string;
    headline: string;
    description: string;
    nsc: string | null;
    news_date: string;
    source: string;
    url: string | null;
}

interface WordPressResponse {
    id?: number;
    [key: string]: any;
}

// ========== Configuration ==========
const CONFIG: Config = {
    groqApiKey: process.env.GROQ_API_KEY || '',
    websiteUrl: process.env.WEBSITE_URL || 'https://pulse.zerodha.com',
    groqModel: process.env.GROQ_MODEL || 'llama3-8b-8192',
    apiInterval: parseInt(process.env.API_INTERVAL || '30000', 10),
    groqRequestDelay: parseInt(process.env.GROQ_REQUEST_DELAY || '1500', 10),
    maxHtmlContentLength: parseInt(process.env.MAX_HTML_CONTENT_LENGTH || '4000', 10),
    wpApiUrl: process.env.WP_API_URL || 'https://profitbooking.in/wp-json/scraper/v1/zerodha',
    wpExtraApiUrl: process.env.WP_EXTRA_API_URL || 'https://profitbooking.in/wp-json/scraper/v1/extra',
    wpUser: process.env.WP_USER,
    wpPass: process.env.WP_PASS,
    googleSheet: {
        sheetId: process.env.SHEET_ID,
        sheetName: process.env.SHEET_NAME || 'Sheet1',
        serviceAccount: {
            email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
            privateKey: process.env.GOOGLE_PRIVATE_KEY?.replace(/\\n/g, '\n')
        }
    },
    groqRateLimit: {
        maxRequests: parseInt(process.env.GROQ_MAX_REQUESTS || '5', 10),
        intervalMs: parseInt(process.env.GROQ_INTERVAL_MS || '65000', 10),
        retryAfterMultiplier: parseFloat(process.env.GROQ_RETRY_AFTER_MULTIPLIER || '1.5')
    },
    maxConsecutiveErrors: parseInt(process.env.MAX_CONSECUTIVE_ERRORS || '3', 10),
    maxRetries: parseInt(process.env.MAX_RETRIES || '3', 10),
    backoffBase: parseInt(process.env.BACKOFF_BASE_MS || '1000', 10),
    maxBackoff: parseInt(process.env.MAX_BACKOFF_MS || '30000', 10)
};

// Validate configuration
if (!CONFIG.groqApiKey) {
    throw new Error('GROQ_API_KEY is required in your .env file.');
}
if (!CONFIG.googleSheet.serviceAccount.email || !CONFIG.googleSheet.serviceAccount.privateKey) {
    throw new Error('Google Sheet service account credentials are required in your .env file.');
}
if (!CONFIG.wpUser || !CONFIG.wpPass) {
    console.warn('WARNING: WordPress credentials are not fully configured.');
}

// ========== EnhancedRateLimiter Class ==========
class EnhancedRateLimiter {
    private maxRequests: number;
    private intervalMs: number;
    private requestTimestamps: number[];
    private pausedUntil: number;

    public getMaxRequests(): number {
        return this.maxRequests;
    }

    constructor(maxRequests: number, intervalMs: number) {
        this.maxRequests = maxRequests;
        this.intervalMs = intervalMs;
        this.requestTimestamps = [];
        this.pausedUntil = 0;
    }

    private async waitForAvailability(): Promise<void> {
        const now = Date.now();
        
        if (now < this.pausedUntil) {
            const waitTime = this.pausedUntil - now;
            await sleep(waitTime);
            return this.waitForAvailability();
        }

        this.requestTimestamps = this.requestTimestamps.filter(
            ts => now - ts < this.intervalMs
        );

        if (this.requestTimestamps.length < this.maxRequests) {
            return;
        }

        const oldestRequest = this.requestTimestamps[0];
        const timeToWait = (this.intervalMs - (now - oldestRequest)) + 100;
        await sleep(timeToWait);
        return this.waitForAvailability();
    }

    async executeRequest<T>(requestFn: () => Promise<T>): Promise<T> {
        await this.waitForAvailability();
        
        const now = Date.now();
        this.requestTimestamps.push(now);
        this.requestTimestamps = this.requestTimestamps.slice(-this.maxRequests);
        
        try {
            return await requestFn();
        } catch (error) {
            if (axios.isAxiosError(error) && error.response?.status === 429) {
                const retryAfter = this.calculateRetryAfter(error);
                this.pausedUntil = now + retryAfter;
                throw error;
            }
            throw error;
        }
    }

    private calculateRetryAfter(error: AxiosError): number {
        const retryAfterHeader = error.response?.headers?.['retry-after'];
        if (retryAfterHeader) {
            const seconds = parseInt(retryAfterHeader, 10);
            let calculatedDelay = seconds * 1000 * CONFIG.groqRateLimit.retryAfterMultiplier;
            return Math.min(calculatedDelay, CONFIG.maxBackoff);
        }
        return Math.min(
            CONFIG.maxBackoff,
            CONFIG.backoffBase * Math.pow(2, this.requestTimestamps.length - this.maxRequests)
        );
    }
}

// ========== NewsProcessor Class ==========
class NewsProcessor {
    private queue: ScrapedItem[];
    private isProcessing: boolean;
    private validNSC: Set<string>;
    private consecutiveGroqErrors: number;
    private rateLimiter: EnhancedRateLimiter;
    private onQueueComplete?: () => void;

    public setOnQueueComplete(callback: () => void): void {
        this.onQueueComplete = callback;
    }

    constructor() {
        this.queue = [];
        this.isProcessing = false;
        this.validNSC = new Set();
        this.consecutiveGroqErrors = 0;
        this.rateLimiter = new EnhancedRateLimiter(
            CONFIG.groqRateLimit.maxRequests,
            CONFIG.groqRateLimit.intervalMs
        );
    }

    async initialize(): Promise<void> {
        await this.loadSymbolsFromSheet();
    }

    private async loadSymbolsFromSheet(): Promise<void> {
        let sheetLoadAttempts = 0;
        while (sheetLoadAttempts < CONFIG.maxRetries) {
            try {
                const auth = new google.auth.JWT({
                    email: CONFIG.googleSheet.serviceAccount.email,
                    key: CONFIG.googleSheet.serviceAccount.privateKey,
                    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
                });

                await auth.authorize();
                const sheets = google.sheets({ version: 'v4', auth });
                const response = await sheets.spreadsheets.values.get({
                    spreadsheetId: CONFIG.googleSheet.sheetId,
                    range: `${CONFIG.googleSheet.sheetName}!A:C`,
                });

                const rows = response.data.values;
                if (!rows || rows.length < 2) {
                    throw new Error('No data found in sheet');
                }

                const headers = rows[0];
                const symbolCol = headers.findIndex(header => header.toLowerCase() === 'symbol');
                if (symbolCol === -1) {
                    throw new Error('Symbol column not found');
                }

                const symbols = rows.slice(1)
                    .map(row => row[symbolCol]?.toString().toUpperCase().trim())
                    .filter((s): s is string => !!s);

                this.validNSC = new Set(symbols);
                return;
            } catch (err) {
                sheetLoadAttempts++;
                if (sheetLoadAttempts >= CONFIG.maxRetries) {
                    throw err;
                }
                const delay = Math.min(
                    CONFIG.maxBackoff,
                    CONFIG.backoffBase * Math.pow(2, sheetLoadAttempts - 1)
                );
                await sleep(delay);
            }
        }
    }

    async addToQueue(items: ScrapedItem | ScrapedItem[]): Promise<void> {
        const itemArray = Array.isArray(items) ? items : [items];
        const newItems = itemArray
            .filter(item => item?.htmlContent)
            .filter(item => !this.queue.some(existing => existing.htmlContent === item.htmlContent))
            .map(item => ({
                ...item,
                htmlContent: this.preprocessHtml(item.htmlContent.substring(0, CONFIG.maxHtmlContentLength))
            }));

        if (newItems.length > 0) {
            this.queue.push(...newItems);
            if (!this.isProcessing) {
                this.processQueue().catch(console.error);
            }
        }
    }

    private preprocessHtml(html: string): string {
        try {
            const dom = new JSDOM(html);
            const doc = dom.window.document;

            const title = doc.querySelector('.title a')?.textContent?.trim() || '';
            const desc = doc.querySelector('.desc')?.textContent?.trim() || '';
            const source = doc.querySelector('.feed')?.textContent?.replace('—', '').trim() || '';
            const url = doc.querySelector('.title a')?.getAttribute('href') || '';

            return `
                <div class="item">
                    <div class="title"><a href="${url}">${title}</a></div>
                    <div class="desc">${desc}</div>
                    <div class="feed">${source}</div>
                </div>
            `;
        } catch (err) {
            return html;
        }
    }

    private async processQueue(): Promise<void> {
        if (this.isProcessing) return;
        this.isProcessing = true;

        try {
            while (this.queue.length > 0) {
                const item = this.queue.shift();
                if (!item) continue;

                try {
                    await this.processItem(item);
                    await sleep(CONFIG.apiInterval / this.rateLimiter.getMaxRequests());
                } catch (err) {
                    console.error('Error processing item:', err);
                    if (this.queue.length < 500) {
                        this.queue.push(item);
                        await sleep(CONFIG.groqRequestDelay * 2);
                    }
                }
            }
        } finally {
            this.isProcessing = false;
            if (this.onQueueComplete && this.queue.length === 0) {
                this.onQueueComplete();
            }
        }
    }

    private async processItem(item: ScrapedItem): Promise<ProcessedResult | null> {
        const aiResult = await this.analyzeWithAI(item);
        if (!aiResult) return null;

        const processedResult: ProcessedResult = {
            company_name: this.cleanseText(aiResult.company_name || 'Unknown Company'),
            headline: this.cleanseText(aiResult.Headline || 'No Title'),
            description: this.cleanseText(aiResult.Body || ''),
            nsc: aiResult.nsc ? aiResult.nsc.toUpperCase().trim() : null,
            news_date: new Date().toISOString().split('T')[0],
            source: this.cleanseText(aiResult.source?.replace('—', '').trim() || 'Unknown'),
            url: aiResult.url?.trim() || null
        };

        if (processedResult.nsc && this.validNSC.has(processedResult.nsc)) {
            await this.storeResult(processedResult);
        } else {
            await this.storeExtraResult(processedResult);
        }

        return processedResult;
    }

    private cleanseText(text: string): string {
        return typeof text !== 'string' ? '' : text
            .replace(/[^\w\s.,-]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .substring(0, 3000);
    }

    private async analyzeWithAI(item: ScrapedItem): Promise<AIResponse | null> {
        const companyList = Array.from(this.validNSC).join(',');
        const prompt = this.createPrompt(item.htmlContent, companyList);
        
        for (let attempt = 1; attempt <= CONFIG.maxRetries; attempt++) {
            try {
                const response = await this.rateLimiter.executeRequest<AxiosResponse<{
                    choices: Array<{ message: { content: string } }>
                }>>(() => axios.post(
                    'https://api.groq.com/openai/v1/chat/completions',
                    {
                        model: CONFIG.groqModel,
                        messages: [{ role: "user", content: prompt }],
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
                ));

                this.consecutiveGroqErrors = 0;
                return this.parseAIResponse(response);
            } catch (err) {
                this.consecutiveGroqErrors++;
                if (this.consecutiveGroqErrors >= CONFIG.maxConsecutiveErrors) {
                    await sleep(300000);
                    this.consecutiveGroqErrors = 0;
                }
                
                if (attempt < CONFIG.maxRetries) {
                    const delay = Math.min(
                        CONFIG.maxBackoff,
                        CONFIG.backoffBase * Math.pow(2, attempt - 1)
                    );
                    await sleep(delay);
                }
            }
        }
        return null;
    }

    private createPrompt(htmlContent: string, companyList: string): string {
        const maxCompanyListLength = 400;
        const truncatedCompanyList = companyList.length > maxCompanyListLength
            ? companyList.substring(0, companyList.lastIndexOf(',', maxCompanyListLength)) + '...'
            : companyList;

        return `Extract stock news to JSON: {company_name: string, Headline: string, Body: string, nsc: string, source: string, url: string}.

**Instructions:**
1. Identify the main company/entity in the news.
2. For 'nsc', find its EXACT symbol **ONLY** from: [${truncatedCompanyList}].
3. Populate other fields from the news content.

HTML: ${htmlContent}`;
    }

    private parseAIResponse(response: AxiosResponse<{
        choices: Array<{ message: { content: string } }>
    }>): AIResponse | null {
        try {
            const content = response.data.choices[0]?.message?.content;
            if (!content) return null;

            const result = JSON.parse(content) as AIResponse;
            if (typeof result.nsc !== 'string' && result.nsc !== null) {
                return null;
            }
            return result;
        } catch (err) {
            console.error('Failed to parse AI response:', err);
            return null;
        }
    }

    private async storeResult(data: ProcessedResult): Promise<void> {
        if (!CONFIG.wpApiUrl || !CONFIG.wpUser || !CONFIG.wpPass) return;

        try {
            const response = await axios.post<WordPressResponse>(
                CONFIG.wpApiUrl,
                data,
                {
                    auth: { username: CONFIG.wpUser, password: CONFIG.wpPass },
                    timeout: 30000
                }
            );
            if (response.data?.id) {
                console.log(`Stored primary item ID ${response.data.id}`);
            }
        } catch (error) {
            console.error('Primary WordPress storage failed:', error);
        }
    }

    private async storeExtraResult(data: ProcessedResult): Promise<void> {
        if (!CONFIG.wpExtraApiUrl || !CONFIG.wpUser || !CONFIG.wpPass) return;

        try {
            const payload = {
                "Headline": data.headline || 'No Headline',
                "Body": data.description || 'No Body',
                "source": data.source,
                "url": data.url
            };

            const response = await axios.post<WordPressResponse>(
                CONFIG.wpExtraApiUrl,
                payload,
                {
                    auth: { username: CONFIG.wpUser, password: CONFIG.wpPass },
                    timeout: 30000
                }
            );
            if (response.data?.id) {
                console.log(`Stored extra item ID ${response.data.id}`);
            }
        } catch (error) {
            console.error('Extra WordPress storage failed:', error);
        }
    }
}

// ========== Browser Management ==========
let browser: Browser | null = null;
let page: Page | null = null;

async function setupBrowser(): Promise<void> {
    if (browser) return;

    browser = await puppeteer.launch({
        headless: "new" as any,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage'
        ],
        timeout: 60000
    });

    page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    page.setDefaultNavigationTimeout(60000);
}

async function closeBrowser(): Promise<void> {
    if (browser) {
        await browser.close();
        browser = null;
        page = null;
    }
}

async function scrapeNews(): Promise<ScrapedItem[]> {
    try {
        await setupBrowser();
        if (!page) throw new Error('Page not initialized');
        
        await page.goto(CONFIG.websiteUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
        await page.waitForSelector('.box.item', { timeout: 30000 });

        return await page.evaluate((): ScrapedItem[] => {
            return Array.from(document.querySelectorAll('.box.item'))
                .map(item => ({
                    htmlContent: item.outerHTML
                }))
                .filter(item => item.htmlContent);
        });
    } catch (err) {
        await closeBrowser();
        return [];
    }
}

// ========== Main Function ==========
async function main(): Promise<void> {
    const processor = new NewsProcessor();
    
    try {
        await processor.initialize();
        await setupBrowser();
        const items = await scrapeNews();

        await new Promise<void>(resolve => {
            processor.setOnQueueComplete(resolve);
            processor.addToQueue(items);
        });
    } catch (err) {
        console.error('Fatal error:', err);
        process.exit(1);
    } finally {
        await closeBrowser();
        process.exit(0);
    }
}

// ========== Process Handlers ==========
process.on('SIGINT', async () => {
    await closeBrowser();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    await closeBrowser();
    process.exit(0);
});

main().catch(err => {
    console.error('Uncaught error:', err);
    process.exit(1);
});