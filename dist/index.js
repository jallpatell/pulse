"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
const puppeteer_1 = __importDefault(require("puppeteer"));
const axios_1 = __importDefault(require("axios"));
const googleapis_1 = require("googleapis");
const dotenv_1 = __importDefault(require("dotenv"));
const jsdom_1 = require("jsdom");
const promises_1 = require("timers/promises");
dotenv_1.default.config();
// ========== Configuration ==========
const CONFIG = {
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
            privateKey: (_a = process.env.GOOGLE_PRIVATE_KEY) === null || _a === void 0 ? void 0 : _a.replace(/\\n/g, '\n')
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
    getMaxRequests() {
        return this.maxRequests;
    }
    constructor(maxRequests, intervalMs) {
        this.maxRequests = maxRequests;
        this.intervalMs = intervalMs;
        this.requestTimestamps = [];
        this.pausedUntil = 0;
    }
    waitForAvailability() {
        return __awaiter(this, void 0, void 0, function* () {
            const now = Date.now();
            if (now < this.pausedUntil) {
                const waitTime = this.pausedUntil - now;
                yield (0, promises_1.setTimeout)(waitTime);
                return this.waitForAvailability();
            }
            this.requestTimestamps = this.requestTimestamps.filter(ts => now - ts < this.intervalMs);
            if (this.requestTimestamps.length < this.maxRequests) {
                return;
            }
            const oldestRequest = this.requestTimestamps[0];
            const timeToWait = (this.intervalMs - (now - oldestRequest)) + 100;
            yield (0, promises_1.setTimeout)(timeToWait);
            return this.waitForAvailability();
        });
    }
    executeRequest(requestFn) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            yield this.waitForAvailability();
            const now = Date.now();
            this.requestTimestamps.push(now);
            this.requestTimestamps = this.requestTimestamps.slice(-this.maxRequests);
            try {
                return yield requestFn();
            }
            catch (error) {
                if (axios_1.default.isAxiosError(error) && ((_a = error.response) === null || _a === void 0 ? void 0 : _a.status) === 429) {
                    const retryAfter = this.calculateRetryAfter(error);
                    this.pausedUntil = now + retryAfter;
                    throw error;
                }
                throw error;
            }
        });
    }
    calculateRetryAfter(error) {
        var _a, _b;
        const retryAfterHeader = (_b = (_a = error.response) === null || _a === void 0 ? void 0 : _a.headers) === null || _b === void 0 ? void 0 : _b['retry-after'];
        if (retryAfterHeader) {
            const seconds = parseInt(retryAfterHeader, 10);
            let calculatedDelay = seconds * 1000 * CONFIG.groqRateLimit.retryAfterMultiplier;
            return Math.min(calculatedDelay, CONFIG.maxBackoff);
        }
        return Math.min(CONFIG.maxBackoff, CONFIG.backoffBase * Math.pow(2, this.requestTimestamps.length - this.maxRequests));
    }
}
// ========== NewsProcessor Class ==========
class NewsProcessor {
    setOnQueueComplete(callback) {
        this.onQueueComplete = callback;
    }
    constructor() {
        this.queue = [];
        this.isProcessing = false;
        this.validNSC = new Set();
        this.consecutiveGroqErrors = 0;
        this.rateLimiter = new EnhancedRateLimiter(CONFIG.groqRateLimit.maxRequests, CONFIG.groqRateLimit.intervalMs);
    }
    initialize() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.loadSymbolsFromSheet();
        });
    }
    loadSymbolsFromSheet() {
        return __awaiter(this, void 0, void 0, function* () {
            let sheetLoadAttempts = 0;
            while (sheetLoadAttempts < CONFIG.maxRetries) {
                try {
                    const auth = new googleapis_1.google.auth.JWT({
                        email: CONFIG.googleSheet.serviceAccount.email,
                        key: CONFIG.googleSheet.serviceAccount.privateKey,
                        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
                    });
                    yield auth.authorize();
                    const sheets = googleapis_1.google.sheets({ version: 'v4', auth });
                    const response = yield sheets.spreadsheets.values.get({
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
                        .map(row => { var _a; return (_a = row[symbolCol]) === null || _a === void 0 ? void 0 : _a.toString().toUpperCase().trim(); })
                        .filter((s) => !!s);
                    this.validNSC = new Set(symbols);
                    return;
                }
                catch (err) {
                    sheetLoadAttempts++;
                    if (sheetLoadAttempts >= CONFIG.maxRetries) {
                        throw err;
                    }
                    const delay = Math.min(CONFIG.maxBackoff, CONFIG.backoffBase * Math.pow(2, sheetLoadAttempts - 1));
                    yield (0, promises_1.setTimeout)(delay);
                }
            }
        });
    }
    addToQueue(items) {
        return __awaiter(this, void 0, void 0, function* () {
            const itemArray = Array.isArray(items) ? items : [items];
            const newItems = itemArray
                .filter(item => item === null || item === void 0 ? void 0 : item.htmlContent)
                .filter(item => !this.queue.some(existing => existing.htmlContent === item.htmlContent))
                .map(item => (Object.assign(Object.assign({}, item), { htmlContent: this.preprocessHtml(item.htmlContent.substring(0, CONFIG.maxHtmlContentLength)) })));
            if (newItems.length > 0) {
                this.queue.push(...newItems);
                if (!this.isProcessing) {
                    this.processQueue().catch(console.error);
                }
            }
        });
    }
    preprocessHtml(html) {
        var _a, _b, _c, _d, _e, _f, _g;
        try {
            const dom = new jsdom_1.JSDOM(html);
            const doc = dom.window.document;
            const title = ((_b = (_a = doc.querySelector('.title a')) === null || _a === void 0 ? void 0 : _a.textContent) === null || _b === void 0 ? void 0 : _b.trim()) || '';
            const desc = ((_d = (_c = doc.querySelector('.desc')) === null || _c === void 0 ? void 0 : _c.textContent) === null || _d === void 0 ? void 0 : _d.trim()) || '';
            const source = ((_f = (_e = doc.querySelector('.feed')) === null || _e === void 0 ? void 0 : _e.textContent) === null || _f === void 0 ? void 0 : _f.replace('—', '').trim()) || '';
            const url = ((_g = doc.querySelector('.title a')) === null || _g === void 0 ? void 0 : _g.getAttribute('href')) || '';
            return `
                <div class="item">
                    <div class="title"><a href="${url}">${title}</a></div>
                    <div class="desc">${desc}</div>
                    <div class="feed">${source}</div>
                </div>
            `;
        }
        catch (err) {
            return html;
        }
    }
    processQueue() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.isProcessing)
                return;
            this.isProcessing = true;
            try {
                while (this.queue.length > 0) {
                    const item = this.queue.shift();
                    if (!item)
                        continue;
                    try {
                        yield this.processItem(item);
                        yield (0, promises_1.setTimeout)(CONFIG.apiInterval / this.rateLimiter.getMaxRequests());
                    }
                    catch (err) {
                        console.error('Error processing item:', err);
                        if (this.queue.length < 500) {
                            this.queue.push(item);
                            yield (0, promises_1.setTimeout)(CONFIG.groqRequestDelay * 2);
                        }
                    }
                }
            }
            finally {
                this.isProcessing = false;
                if (this.onQueueComplete && this.queue.length === 0) {
                    this.onQueueComplete();
                }
            }
        });
    }
    processItem(item) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            const aiResult = yield this.analyzeWithAI(item);
            if (!aiResult)
                return null;
            const processedResult = {
                company_name: this.cleanseText(aiResult.company_name || 'Unknown Company'),
                headline: this.cleanseText(aiResult.Headline || 'No Title'),
                description: this.cleanseText(aiResult.Body || ''),
                nsc: aiResult.nsc ? aiResult.nsc.toUpperCase().trim() : null,
                news_date: new Date().toISOString().split('T')[0],
                source: this.cleanseText(((_a = aiResult.source) === null || _a === void 0 ? void 0 : _a.replace('—', '').trim()) || 'Unknown'),
                url: ((_b = aiResult.url) === null || _b === void 0 ? void 0 : _b.trim()) || null
            };
            if (processedResult.nsc && this.validNSC.has(processedResult.nsc)) {
                yield this.storeResult(processedResult);
            }
            else {
                yield this.storeExtraResult(processedResult);
            }
            return processedResult;
        });
    }
    cleanseText(text) {
        return typeof text !== 'string' ? '' : text
            .replace(/[^\w\s.,-]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .substring(0, 3000);
    }
    analyzeWithAI(item) {
        return __awaiter(this, void 0, void 0, function* () {
            const companyList = Array.from(this.validNSC).join(',');
            const prompt = this.createPrompt(item.htmlContent, companyList);
            for (let attempt = 1; attempt <= CONFIG.maxRetries; attempt++) {
                try {
                    const response = yield this.rateLimiter.executeRequest(() => axios_1.default.post('https://api.groq.com/openai/v1/chat/completions', {
                        model: CONFIG.groqModel,
                        messages: [{ role: "user", content: prompt }],
                        temperature: 0.2,
                        response_format: { type: "json_object" },
                        max_tokens: 1000
                    }, {
                        headers: {
                            'Authorization': `Bearer ${CONFIG.groqApiKey}`,
                            'Content-Type': 'application/json'
                        },
                        timeout: 30000
                    }));
                    this.consecutiveGroqErrors = 0;
                    return this.parseAIResponse(response);
                }
                catch (err) {
                    this.consecutiveGroqErrors++;
                    if (this.consecutiveGroqErrors >= CONFIG.maxConsecutiveErrors) {
                        yield (0, promises_1.setTimeout)(300000);
                        this.consecutiveGroqErrors = 0;
                    }
                    if (attempt < CONFIG.maxRetries) {
                        const delay = Math.min(CONFIG.maxBackoff, CONFIG.backoffBase * Math.pow(2, attempt - 1));
                        yield (0, promises_1.setTimeout)(delay);
                    }
                }
            }
            return null;
        });
    }
    createPrompt(htmlContent, companyList) {
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
    parseAIResponse(response) {
        var _a, _b;
        try {
            const content = (_b = (_a = response.data.choices[0]) === null || _a === void 0 ? void 0 : _a.message) === null || _b === void 0 ? void 0 : _b.content;
            if (!content)
                return null;
            const result = JSON.parse(content);
            if (typeof result.nsc !== 'string' && result.nsc !== null) {
                return null;
            }
            return result;
        }
        catch (err) {
            console.error('Failed to parse AI response:', err);
            return null;
        }
    }
    storeResult(data) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            if (!CONFIG.wpApiUrl || !CONFIG.wpUser || !CONFIG.wpPass)
                return;
            try {
                const response = yield axios_1.default.post(CONFIG.wpApiUrl, data, {
                    auth: { username: CONFIG.wpUser, password: CONFIG.wpPass },
                    timeout: 30000
                });
                if ((_a = response.data) === null || _a === void 0 ? void 0 : _a.id) {
                    console.log(`Stored primary item ID ${response.data.id}`);
                }
            }
            catch (error) {
                console.error('Primary WordPress storage failed:', error);
            }
        });
    }
    storeExtraResult(data) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            if (!CONFIG.wpExtraApiUrl || !CONFIG.wpUser || !CONFIG.wpPass)
                return;
            try {
                const payload = {
                    "Headline": data.headline || 'No Headline',
                    "Body": data.description || 'No Body',
                    "source": data.source,
                    "url": data.url
                };
                const response = yield axios_1.default.post(CONFIG.wpExtraApiUrl, payload, {
                    auth: { username: CONFIG.wpUser, password: CONFIG.wpPass },
                    timeout: 30000
                });
                if ((_a = response.data) === null || _a === void 0 ? void 0 : _a.id) {
                    console.log(`Stored extra item ID ${response.data.id}`);
                }
            }
            catch (error) {
                console.error('Extra WordPress storage failed:', error);
            }
        });
    }
}
// ========== Browser Management ==========
let browser = null;
let page = null;
function setupBrowser() {
    return __awaiter(this, void 0, void 0, function* () {
        if (browser)
            return;
        browser = yield puppeteer_1.default.launch({
            headless: "new",
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage'
            ],
            timeout: 60000
        });
        page = yield browser.newPage();
        yield page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        page.setDefaultNavigationTimeout(60000);
    });
}
function closeBrowser() {
    return __awaiter(this, void 0, void 0, function* () {
        if (browser) {
            yield browser.close();
            browser = null;
            page = null;
        }
    });
}
function scrapeNews() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            yield setupBrowser();
            if (!page)
                throw new Error('Page not initialized');
            yield page.goto(CONFIG.websiteUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
            yield page.waitForSelector('.box.item', { timeout: 30000 });
            return yield page.evaluate(() => {
                return Array.from(document.querySelectorAll('.box.item'))
                    .map(item => ({
                    htmlContent: item.outerHTML
                }))
                    .filter(item => item.htmlContent);
            });
        }
        catch (err) {
            yield closeBrowser();
            return [];
        }
    });
}
// ========== Main Function ==========
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const processor = new NewsProcessor();
        try {
            yield processor.initialize();
            yield setupBrowser();
            const items = yield scrapeNews();
            yield new Promise(resolve => {
                processor.setOnQueueComplete(resolve);
                processor.addToQueue(items);
            });
        }
        catch (err) {
            console.error('Fatal error:', err);
            process.exit(1);
        }
        finally {
            yield closeBrowser();
            process.exit(0);
        }
    });
}
// ========== Process Handlers ==========
process.on('SIGINT', () => __awaiter(void 0, void 0, void 0, function* () {
    yield closeBrowser();
    process.exit(0);
}));
process.on('SIGTERM', () => __awaiter(void 0, void 0, void 0, function* () {
    yield closeBrowser();
    process.exit(0);
}));
main().catch(err => {
    console.error('Uncaught error:', err);
    process.exit(1);
});
