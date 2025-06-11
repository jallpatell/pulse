import puppeteer from 'puppeteer';
import axios from 'axios';
import { google } from 'googleapis';
import dotenv from 'dotenv';
import { JSDOM } from 'jsdom';
import { setTimeout as sleep } from 'timers/promises';

dotenv.config();


const CONFIG = {
    groqApiKey: process.env.GROQ_API_KEY,
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


if (!CONFIG.groqApiKey) {
    throw new Error('GROQ_API_KEY is required in your .env file.');
}
if (!CONFIG.googleSheet.serviceAccount.email || !CONFIG.googleSheet.serviceAccount.privateKey) {
    throw new Error('Google Sheet service account credentials (GOOGLE_SERVICE_ACCOUNT_EMAIL, GOOGLE_PRIVATE_KEY) are required in your .env file.');
}
if (!CONFIG.wpUser || !CONFIG.wpPass) {
    console.warn('WARNING: WordPress credentials (WP_USER, WP_PASS) are not fully configured. WordPress API calls might fail.');
}


class EnhancedRateLimiter {
    constructor(maxRequests, intervalMs) {
        this.maxRequests = maxRequests;
        this.intervalMs = intervalMs;
        this.requestTimestamps = [];
        this.pausedUntil = 0;
        console.log(`RateLimiter: Configured for ${maxRequests} requests per ${intervalMs / 1000}s`);
    }

    async waitForAvailability() {
        const now = Date.now();
        
        
        if (now < this.pausedUntil) {
            const waitTime = this.pausedUntil - now;
            console.log(`RateLimiter: Explicitly paused. Waiting for ${waitTime}ms.`);
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
        
        console.log(`RateLimiter: Rate limit hit (${this.requestTimestamps.length}/${this.maxRequests} requests in ${this.intervalMs / 1000}s). Waiting ${timeToWait}ms...`);
        await sleep(timeToWait);
       
        return this.waitForAvailability(); 
    }

    async executeRequest(requestFn) {
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
                console.warn(`RateLimiter: 429 received from API. Pausing for ${retryAfter}ms as instructed.`);
               
                throw error; 
            }
            
            throw error;
        }
    }

    calculateRetryAfter(error) {
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

class NewsProcessor {
    constructor() {
        this.queue = [];
        this.isProcessing = false;
        this.validNSC = new Set();
        this.consecutiveGroqErrors = 0;
        this.rateLimiter = new EnhancedRateLimiter(
            CONFIG.groqRateLimit.maxRequests,
            CONFIG.groqRateLimit.intervalMs
        );
        console.log('NewsProcessor initialized with enhanced rate limiting.');
    }

    async initialize() {
        console.log('NewsProcessor: Initializing...');
        await this.loadSymbolsFromSheet();
        console.log(`NewsProcessor initialized with ${this.validNSC.size} symbols from Google Sheet.`);
    }

    async loadSymbolsFromSheet() {
        let sheetLoadAttempts = 0; 
        while (sheetLoadAttempts < CONFIG.maxRetries) {
            try {
                console.log(`NewsProcessor: Loading symbols from Google Sheet (attempt ${sheetLoadAttempts + 1})...`);
                const auth = new google.auth.JWT({
                    email: CONFIG.googleSheet.serviceAccount.email,
                    key: CONFIG.googleSheet.serviceAccount.privateKey,
                    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
                });

                await auth.authorize();
                console.log('NewsProcessor: Google Sheet authentication successful.');

                const sheets = google.sheets({ version: 'v4', auth });
                const response = await sheets.spreadsheets.values.get({
                    spreadsheetId: CONFIG.googleSheet.sheetId,
                    range: `${CONFIG.googleSheet.sheetName}!A:C`,
                    
                });

                const rows = response.data.values;
                if (!rows || rows.length < 2) {
                    throw new Error('No data found in sheet or headers missing.');
                }

                const headers = rows[0];
                const symbolCol = headers.findIndex(header => header.toLowerCase() === 'symbol');
                if (symbolCol === -1) {
                    throw new Error('Symbol column not found in sheet headers.');
                }

                const symbols = rows.slice(1)
                    .map(row => row[symbolCol]?.toString().toUpperCase().trim())
                    .filter(s => s); 

                this.validNSC = new Set(symbols);
                console.log(`NewsProcessor: Successfully loaded ${this.validNSC.size} symbols.`);
                return; 
            } catch (err) {
                sheetLoadAttempts++;
                console.error(`NewsProcessor: Failed to load symbols (attempt ${sheetLoadAttempts}/${CONFIG.maxRetries}):`, err.message);

                if (sheetLoadAttempts >= CONFIG.maxRetries) {
                    throw new Error(`NewsProcessor: Max retry attempts reached for loading symbols. Last error: ${err.message}`);
                }

                const delay = Math.min(
                    CONFIG.maxBackoff,
                    CONFIG.backoffBase * Math.pow(2, sheetLoadAttempts - 1)
                );
                console.log(`NewsProcessor: Retrying symbol load in ${delay / 1000} seconds...`);
                await sleep(delay);
            }
        }
    }

    async addToQueue(items) {
        if (!Array.isArray(items)) items = [items];
        console.log(`NewsProcessor: Received ${items.length} potential items to add to queue.`);

        const newItems = items
            .filter(item => item?.htmlContent) 
            
            .filter(item => !this.queue.some(existing => existing.htmlContent === item.htmlContent))
            .map(item => ({
                ...item,
                
                htmlContent: this.preprocessHtml(item.htmlContent.substring(0, CONFIG.maxHtmlContentLength))
            }));

        if (newItems.length > 0) {
            this.queue.push(...newItems);
            console.log(`NewsProcessor: Added ${newItems.length} new items to queue. Total queue size: ${this.queue.length}.`);
            
            if (!this.isProcessing) {
                console.log('NewsProcessor: Starting queue processing.');
                
                this.processQueue().catch(err => {
                    console.error('NewsProcessor: Critical error during queue processing:', err);
                });
            }
        } else {
            console.log('NewsProcessor: No new items to add to queue or all are duplicates.');
        }
    }

    preprocessHtml(html) {
        try {
            const dom = new JSDOM(html);
            const doc = dom.window.document;

           
            const title = doc.querySelector('.title a')?.textContent?.trim() || '';
            const desc = doc.querySelector('.desc')?.textContent?.trim() || '';
            const source = doc.querySelector('.feed')?.textContent?.replace('—', '').trim() || '';
            const url = doc.querySelector('.title a')?.getAttribute('href') || '';

            
            const processedHtml = `
                <div class="item">
                    <div class="title"><a href="${url}">${title}</a></div>
                    <div class="desc">${desc}</div>
                    <div class="feed">${source}</div>
                </div>
            `;
            return processedHtml;
        } catch (err) {
            console.error('NewsProcessor: HTML preprocessing failed:', err.message);
            return html; 
        }
    }//

    async processQueue() {
        
        if (this.isProcessing) return; 
        this.isProcessing = true;
        console.log('NewsProcessor: Beginning queue processing loop.');

        try {
            while (this.queue.length > 0) {
                const item = this.queue.shift(); 
                console.log(`NewsProcessor: Processing item from queue. ${this.queue.length} items remaining.`);
                
                try {
                    await this.processItem(item);
                    
                    await sleep(CONFIG.apiInterval / this.rateLimiter.maxRequests);  
                } catch (err) {
                    console.error('NewsProcessor: Error processing single item:', err.message);
                   
                    if (this.queue.length < 500) { 
                        
                        console.log('NewsProcessor: Requeuing item for retry later.');
                        this.queue.push(item); 
                        await sleep(CONFIG.groqRequestDelay * 2); 
                    } else {
                        console.warn('NewsProcessor: Not requeuing item due to large queue size or persistent errors.');
                    }
                }
            }
        } catch (queueError) {
            console.error('NewsProcessor: Unhandled error during queue processing loop:', queueError);
        } finally {
            this.isProcessing = false;
            console.log('NewsProcessor: Queue processing finished.');
           
            if (typeof this.onQueueComplete === 'function' && this.queue.length === 0) {
                this.onQueueComplete();
            }
        }
    }

    async processItem(item) {
        console.log('NewsProcessor: Analyzing item with AI...');
        const aiResult = await this.analyzeWithAI(item);

        if (!aiResult) {
            console.log('NewsProcessor: AI analysis returned null or failed.');
            return null; 
        }

        const processedResult = {
            company_name: this.cleanseText(aiResult.company_name || 'Unknown Company'),
            headline: this.cleanseText(aiResult.headline || 'No Title'),
            description: this.cleanseText(aiResult.description || ''),
            nsc: aiResult.nsc ? aiResult.nsc.toUpperCase().trim() : null,
            news_date: new Date().toISOString().split('T')[0],
            source: this.cleanseText(aiResult.source?.replace('—', '').trim() || 'Unknown'),
            url: aiResult.url?.trim() || null
        };

        console.log(`NewsProcessor: AI analysis returned NSC: '${processedResult.nsc}'`);

        if (processedResult.nsc && this.validNSC.has(processedResult.nsc)) {
            console.log(`NewsProcessor: Valid NSC found: ${processedResult.nsc}. Storing to primary endpoint.`);
            await this.storeResult(processedResult);
        } else {
            console.log(`NewsProcessor: NSC '${processedResult.nsc}' is null or not in valid list. Storing to extra endpoint.`);
            await this.storeExtraResult(processedResult);
        }

        return processedResult;
    }

    cleanseText(text) {
        
        if (typeof text !== 'string') return ''; 
        return text
            
            .replace(/[^\w\s.,-]/g, ' ') 
           
            .replace(/\s+/g, ' ') 
            .trim()
            .substring(0, 3000); 
    }

    async analyzeWithAI(item) {
        const companyList = Array.from(this.validNSC).join(',');
        const prompt = this.createPrompt(item.htmlContent, companyList);
        
        for (let attempt = 1; attempt <= CONFIG.maxRetries; attempt++) {
            try {
                console.log(`NewsProcessor: Sending AI request (attempt ${attempt}/${CONFIG.maxRetries})...`);
                
                
                const response = await this.rateLimiter.executeRequest(async () => {
                    
                    await sleep(CONFIG.groqRequestDelay); 
                    return axios.post(
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
                    );
                });

                this.consecutiveGroqErrors = 0; 
                console.log('NewsProcessor: Groq API call successful.');
                return this.parseAIResponse(response);
            } catch (err) {
                
                const errorMessage = axios.isAxiosError(err) ? 
                                     err.response?.data?.error?.message || err.message : 
                                     err.message;
                console.error(`NewsProcessor: AI request failed (attempt ${attempt}/${CONFIG.maxRetries}):`, errorMessage);

                
                
                this.consecutiveGroqErrors++;

                if (this.consecutiveGroqErrors >= CONFIG.maxConsecutiveErrors) {
                    console.error(`NewsProcessor: Too many consecutive Groq errors (${this.consecutiveGroqErrors}). Pausing for 5 minutes (300000ms) before resetting counter...`);
                    await sleep(300000); 
                    this.consecutiveGroqErrors = 0; 
                }
                
                if (attempt < CONFIG.maxRetries) {
                    const delay = Math.min(
                        CONFIG.maxBackoff,
                        CONFIG.backoffBase * Math.pow(2, attempt - 1)
                    );
                    console.log(`NewsProcessor: Retrying Groq API call in ${delay / 1000} seconds...`);
                    await sleep(delay);
                }
            }
        }
        console.error('NewsProcessor: Max retry attempts reached for Groq AI call. Giving up on this item.');
        return null;
    }

    createPrompt(htmlContent, companyList) {
        const maxCompanyListLength = 400; 
        const truncatedCompanyList = companyList.length > maxCompanyListLength
            ? companyList.substring(0, companyList.lastIndexOf(',', maxCompanyListLength)) + '...'
            : companyList;

        return `Extract stock news to JSON: {company_name: string, Headline: string, Body: string, nsc: string, source: string, url: string}.

**Instructions:**
1.  Identify the main company/entity in the news.
2.  For 'nsc', find its EXACT symbol **ONLY** from: [${truncatedCompanyList}]. **CRITICAL**: If no exact match *from this list* or not about a listed company from this list, 'nsc' **MUST be null**. **NEVER** generate an 'nsc' not in the provided list.
3.  Populate 'company_name', 'Headline', 'Body', 'source', 'url' from the news. If 'nsc' is null, fill relevant fields; otherwise, null if content is irrelevant.

HTML: ${htmlContent}`;
    }

    parseAIResponse(response) {
        try {
            console.log('NewsProcessor: Parsing AI response...');
            const content = response.data.choices[0]?.message?.content;
            if (!content) {
                console.warn('NewsProcessor: AI response content is empty.');
                return null;
            }

            const result = JSON.parse(content);
            
            if (typeof result !== 'object' || result === null || 
                (typeof result.nsc !== 'string' && result.nsc !== null)) { 
                console.warn('NewsProcessor: AI response JSON is invalid or missing/incorrect NSC. Raw AI response:', content);
                return null;
            }
            
          
            result.headline = result.Headline;
            delete result.Headline;
            result.description = result.Body;
            delete result.Body;
            return result;
        } catch (err) {
            console.error('NewsProcessor: Failed to parse AI response JSON:', err.message);
            console.error('Raw AI content that failed to parse:', response.data.choices[0]?.message?.content);
            return null;
        }
    }

    async storeResult(data) {
        if (!CONFIG.wpApiUrl || !CONFIG.wpUser || !CONFIG.wpPass) {
            console.log('NewsProcessor: Primary WordPress storage skipped (WP_API_URL or credentials not fully configured).');
            return;
        }

        try {
            console.log(`NewsProcessor: Attempting to store primary data for NSC: ${data.nsc} to WordPress.`);
            const response = await axios.post(CONFIG.wpApiUrl, data, {
                auth: { username: CONFIG.wpUser, password: CONFIG.wpPass },
                timeout: 30000 
            });

            if (response.data?.id) {
                console.log(`NewsProcessor: Successfully stored primary item ID ${response.data.id} for ${data.nsc} - "${data.headline.substring(0, 30)}..."`);
            } else {
                console.warn('NewsProcessor: Primary WordPress storage responded without an ID in data:', response.data);
            }
        } catch (error) {
            console.error('NewsProcessor: Primary WordPress storage failed:', error.response?.data?.message || error.message);
            if (axios.isAxiosError(error) && error.response) {
                console.error('WordPress Primary API Error Response Status:', error.response.status);
                console.error('WordPress Primary API Error Response Data:', error.response.data);
            }
            console.error('Failed primary data:', data);
        }
    }

    async storeExtraResult(data) {
        if (!CONFIG.wpExtraApiUrl || !CONFIG.wpUser || !CONFIG.wpPass) {
            console.log('NewsProcessor: Extra WordPress storage skipped (WP_EXTRA_API_URL or credentials not fully configured).');
            return;
        }

        try {
            console.log(`NewsProcessor: Attempting to store extra data (NSC null/invalid) to WordPress.`);
            
           
            const payload = {
                "Headline": data.headline || 'No Headline Provided',
                "Body": data.description || 'No Body Provided',
                "source": data.source,
                "url": data.url
            };

            console.log("NewsProcessor: Sending extra data payload:", payload); 

            const response = await axios.post(CONFIG.wpExtraApiUrl, payload, {
                auth: { username: CONFIG.wpUser, password: CONFIG.wpPass },
                timeout: 30000
            });

            if (response.data?.id) {
                console.log(`NewsProcessor: Successfully stored extra item ID ${response.data.id} for "${data.headline.substring(0, 30)}..."`);
            } else {
                console.warn('NewsProcessor: Extra WordPress storage responded without an ID in data:', response.data);
            }
        } catch (error) {
            console.error('NewsProcessor: Extra WordPress storage failed:', error.response?.data?.message || error.message);
            if (axios.isAxiosError(error) && error.response) {
                console.error('WordPress Extra API Error Response Status:', error.response.status);
                console.error('WordPress Extra API Error Response Data:', error.response.data);
            }
            console.error('Failed extra data:', data);
        }
    }
}


let browser, page;

async function setupBrowser() {
    if (browser) {
        console.log('Browser: Browser already set up and active.');
        return;
    }
    console.log('Browser: Launching Puppeteer browser...');
    try {
        browser = await puppeteer.launch({
            headless: 'new', 
            args: [
                '--no-sandbox', 
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage' 
            ],
            timeout: 60000 // 1 minute timeout for browser launch
        });
        console.log('Browser: Puppeteer browser launched successfully.');

        page = await browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        page.setDefaultNavigationTimeout(60000); // 1 minute timeout for page navigation
        console.log('Browser: New page created and configured.');
    } catch (err) {
        console.error('Browser: Failed to launch Puppeteer browser:', err.message);
        throw err; // Re-throw to halt execution if browser fails to launch
    }
}

async function closeBrowser() {
    if (browser) {
        console.log('Browser: Closing Puppeteer browser...');
        try {
            await browser.close();
            console.log('Browser: Puppeteer browser closed.');
        } catch (err) {
            console.error('Browser: Error closing Puppeteer browser:', err.message);
        } finally {
            browser = null;
            page = null;
        }
    } else {
        console.log('Browser: No active browser to close.');
    }
}

async function scrapeNews() {
    try {
        await setupBrowser(); // Ensure browser is set up before scraping
        console.log(`Scraper: Navigating to ${CONFIG.websiteUrl}...`);
        await page.goto(CONFIG.websiteUrl, { waitUntil: 'domcontentloaded', timeout: 45000 }); // Increased goto timeout
        console.log('Scraper: Page loaded. Waiting for selector ".box.item"...');
        await page.waitForSelector('.box.item', { timeout: 30000 }); // Wait for news items to appear
        console.log('Scraper: Selector ".box.item" found. Extracting HTML content.');

        const scrapedItems = await page.evaluate(() => {
            return Array.from(document.querySelectorAll('.box.item'))
                .map(item => ({
                    htmlContent: item.outerHTML
                }))
                .filter(item => item.htmlContent); // Ensure htmlContent is not empty
        });
        console.log(`Scraper: Found ${scrapedItems.length} news items.`);
        return scrapedItems;
    } catch (err) {
        console.error('Scraper: Scraping failed:', err.message);
        // It's crucial to close the browser if scraping fails to prevent orphaned processes
        await closeBrowser(); 
        return []; // Return empty array to allow the main process to continue if possible
    }
}


async function main() {
    console.log('Main: Starting application with enhanced rate limiting and error handling...');
    const processor = new NewsProcessor();
    
    try {
        
        await processor.initialize();
        // Setup browser (only once)
        await setupBrowser(); 

        console.log('Main: Performing initial news scrape.');
        const items = await scrapeNews();

        
        await new Promise(resolve => {
            processor.onQueueComplete = resolve;
            processor.addToQueue(items); 
        });

        console.log('Main: All articles processed or no new articles found. Exiting.');
    } catch (err) {
        console.error('Main: Fatal application error during startup or main loop:', err);
        process.exit(1); // Exit with error code
    } finally {
       
        await closeBrowser(); 
        process.exit(0); // Exit gracefully
    }
}


process.on('SIGINT', async () => {
    console.log('System: SIGINT signal received. Shutting down gracefully...');
    await closeBrowser();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('System: SIGTERM signal received. Shutting down gracefully...');
    await closeBrowser();
    process.exit(0);
});

// Start the main function
main().catch(err => {
   
    console.error('Main: Uncaught error in main execution flow:', err);
    process.exit(1);
});
