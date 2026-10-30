/**
 * 后台生成 / Background Generation
 *
 * 需要配合服务端插件 SillyTavern-BackgroundGen-Server（插件 id: bg-gen）使用：
 * - 生成请求改由服务端中转。页面在线时一切照旧（SillyTavern 自己处理流式、正则、思维链、事件、保存）；
 *   断网、锁屏、切后台后回来，从断点续传，SillyTavern 感觉不到中断。
 * - 关掉/刷新页面后，服务端照样把回复生成完。下次打开这个聊天时，用 SillyTavern 自己的
 *   收尾流程（cleanUpMessage → saveReply → MESSAGE_RECEIVED 等事件 → 保存）把回复写进聊天。
 * - 服务端插件不可用时自动退回原始行为。
 */
import * as ST from '../../../../script.js';
import { extension_settings } from '../../../extensions.js';
import * as Groups from '../../../group-chats.js';
import * as OpenAI from '../../../openai.js';
import * as Reasoning from '../../../reasoning.js';
import * as Regex from '../../regex/engine.js';
import * as Textgen from '../../../textgen-settings.js';
import * as Kobold from '../../../kai-settings.js';
import * as Novel from '../../../nai-settings.js';
import { power_user } from '../../../power-user.js';
import EventSourceStream from '../../../sse-stream.js';
import { Popup, POPUP_TYPE, POPUP_RESULT } from '../../../popup.js';

const VERSION = '1.0.2';
const LOG = '[BG]';
const MODULE = 'bgGen';
const API = '/api/plugins/bg-gen';
const TARGETS = new Set([
    '/api/backends/chat-completions/generate',
    '/api/backends/text-completions/generate',
    '/api/backends/kobold/generate',
    '/api/novelai/generate',
]);
const CC_PATH = '/api/backends/chat-completions/generate';
const MAIN_TYPES = new Set(['normal', 'regenerate', 'swipe', 'continue']);
const SAVE_PATHS = new Set(['/api/chats/save', '/api/chats/group/save']);
const ACK_FALLBACK_MS = 60_000;
const DONE_GRACE_MS = 20_000;

extension_settings[MODULE] ??= {};
const settings = extension_settings[MODULE];
settings.enabled ??= true;
settings.notify ??= true;

// ------------------------------------------------------------------ state

const nativeFetch = window.fetch;
const clientId = randomId();
let pluginState = 'unknown'; // unknown | ok | missing
/** @type {Map<string, RelayJob>} */
const activeJobs = new Map();
/** 最近一次「真正的」生成（非 quiet / dryRun），用于判断哪个请求是主回复 */
let mainGen = null;
/** SillyTavern 刚组装好、马上要发出的请求（CHAT/TEXT_COMPLETION_SETTINGS_READY），用来精确认出主回复请求 */
let lastPrepared = null;
let lastBeforeUnload = 0;
let pageHidden = false;
/** 让 ST 自己的流式解析函数「重放」已缓存的结果 */
let replayNext = null;
/** 本页已处理（写入 / 丢弃）的任务 */
const settled = new Set();
/** 用户选了「以后再说」的任务：切换聊天后再问 */
const deferred = new Set();
let checking = false;
let recheckTimer = null;
let reattach = null;
const wakeWaiters = new Set();
let lastJobs = [];
/** 页面刚加载时聊天可能还没自动打开，先别提示「其他聊天有回复」 */
let notifyReady = false;
/** 本页的聊天内容已过期（回复已由别的页面写入）：在重新加载之前拦下本页的保存，免得旧内容覆盖新内容 */
let saveQuarantineUntil = 0;
/** 每次切换聊天加一，用来区分「真的换了聊天」和加载过程中的短暂状态 */
let chatEpoch = 0;

// ------------------------------------------------------------------ utils

function randomId() {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
}

function hash(str) {
    str = String(str ?? '');
    let h1 = 0xdeadbeef, h2 = 0x41c6ce57;
    for (let i = 0; i < str.length; i++) {
        const ch = str.charCodeAt(i);
        h1 = Math.imul(h1 ^ ch, 2654435761);
        h2 = Math.imul(h2 ^ ch, 1597334677);
    }
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36);
}

function esc(value) {
    return String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', '\'': '&#39;' })[c]);
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

function abortError() {
    return new DOMException('The user aborted a request.', 'AbortError');
}

class LostError extends Error {
    constructor() {
        super('这条回复已由另一个页面接收');
        this.name = 'LostError';
    }
}

function fatal(message) {
    const error = new Error(message);
    error.bgFatal = true;
    return error;
}

function requestInfo(input, init) {
    try {
        const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : input?.url;
        const url = new URL(raw, location.href);
        if (url.origin !== location.origin) return null;
        const method = String(init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
        return { path: url.pathname, method };
    } catch {
        return null;
    }
}

function headersObject() {
    try {
        return ST.getRequestHeaders();
    } catch {
        return { 'Content-Type': 'application/json' };
    }
}

/** 带超时的 fetch：手机网络「半死」时请求可能永远挂着，不能让它卡住整个流程 */
async function fetchWithTimeout(url, init = {}, ms = 15_000) {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), ms);
    try {
        return await nativeFetch(url, { ...init, signal: ac.signal });
    } finally {
        clearTimeout(timer);
    }
}

async function api(path, { method = 'GET', body, keepalive = false } = {}) {
    const init = {
        method,
        headers: headersObject(),
        body: body === undefined ? undefined : JSON.stringify(body),
        cache: 'no-store',
    };
    const res = keepalive ? await nativeFetch(`${API}${path}`, { ...init, keepalive }) : await fetchWithTimeout(`${API}${path}`, init);
    let data = null;
    try { data = await res.json(); } catch { /* ignore */ }
    return { ok: res.ok, status: res.status, data };
}

/** 等待网络恢复 / 页面重新可见 / 超时，三者先到为准 */
function wake(ms) {
    return new Promise(resolve => {
        const done = () => { clearTimeout(timer); wakeWaiters.delete(done); resolve(); };
        const timer = setTimeout(done, ms);
        wakeWaiters.add(done);
    });
}

function wakeAll() {
    for (const fn of [...wakeWaiters]) fn();
}

function isGenerating() {
    return !!(ST.is_send_press || ST.streamingProcessor || document.body.dataset.generating);
}

function fp(m) {
    if (!m) return null;
    return {
        u: !!m.is_user,
        n: m.name ?? '',
        d: m.send_date ?? '',
        h: hash(m.mes),
        h0: Array.isArray(m.swipes) && m.swipes.length ? hash(m.swipes[0]) : null,
    };
}

function fpMatch(m, f) {
    if (!f) return true;
    if (!m) return false;
    if (!!m.is_user !== f.u || (m.name ?? '') !== f.n) return false;
    return (f.d && m.send_date === f.d) || hash(m.mes) === f.h || (f.h0 !== null && Array.isArray(m.swipes) && hash(m.swipes[0]) === f.h0);
}

function currentChatRef() {
    const group = Groups.selected_group || null;
    const id = ST.getCurrentChatId?.();
    if (!id) return null;
    if (group) {
        const g = Groups.groups?.find(x => x.id === group);
        return { id, group, avatar: null, label: g?.name || '群聊' };
    }
    const ch = ST.characters?.[ST.this_chid];
    return ch ? { id, group: null, avatar: ch.avatar, label: ch.name } : null;
}

function sameChat(ref) {
    const cur = currentChatRef();
    if (!cur || !ref) return false;
    return cur.id === ref.id && (cur.group || null) === (ref.group || null) && (ref.group ? true : cur.avatar === ref.avatar);
}

// ------------------------------------------------------------------ meta (what is this request?)

function buildMeta(path, payload) {
    const isCC = path === CC_PATH;
    const meta = {
        v: 1,
        client: clientId,
        path,
        kind: 'aux',
        api: isCC ? 'openai' : (ST.main_api || ''),
        source: isCC ? payload.chat_completion_source : null,
        textgenType: Textgen.textgenerationwebui_settings?.type ?? null,
        stream: !!payload.stream,
        type: isCC ? (payload.type || 'normal') : (mainGen?.type || 'quiet'),
        multiSwipe: isCC && Number(payload.n) > 1,
        createdAt: Date.now(),
    };

    const gen = mainGen;
    const fresh = gen && !gen.consumed && Date.now() - gen.at < 15 * 60_000;
    const sp = ST.streamingProcessor;
    const chatRef = currentChatRef();
    const prep = lastPrepared && Date.now() - lastPrepared.at < 15_000 ? lastPrepared : null;
    let isMain = false;
    if (fresh && chatRef && MAIN_TYPES.has(gen.type)) {
        if (isCC) {
            // 必须正是 SillyTavern 刚为这次生成组装的那个请求（脚本 / 插件自己发的请求不算）
            isMain = meta.type === gen.type
                && !!prep && prep.api === 'openai' && prep.type === meta.type
                && prep.n === (Array.isArray(payload.messages) ? payload.messages.length : -1)
                && prep.stream === meta.stream
                && (!meta.stream || (sp && sp.messageId === -1));
        } else if (path === '/api/backends/text-completions/generate') {
            isMain = !!prep && prep.api === 'textgen' && prep.promptLength === String(payload.prompt ?? '').length
                && (meta.stream ? !!(sp && sp.messageId === -1 && sp.type === gen.type) : !sp);
        } else {
            isMain = meta.stream ? !!(sp && sp.messageId === -1 && sp.type === gen.type) : !sp;
        }
    }
    if (!isMain) return meta;

    gen.consumed = true;
    lastPrepared = null;
    const chat = ST.chat;
    const type = gen.type;
    const index = (type === 'swipe' || type === 'continue') ? chat.length - 1 : chat.length;
    const target = chat[index];
    const prev = chat[index - 1];
    meta.kind = 'main';
    meta.type = type;
    meta.startedAt = gen.at;
    meta.chat = chatRef;
    meta.speaker = {
        name: ST.name2,
        avatar: ST.characters?.[ST.this_chid]?.avatar ?? null,
        genId: Groups.group_generation_id ?? null,
    };
    meta.genApi = safe(() => ST.getGeneratingApi());
    meta.genModel = safe(() => ST.getGeneratingModel());
    meta.showThoughts = OpenAI.oai_settings?.show_thoughts !== false;
    meta.anchor = {
        index,
        length: chat.length,
        prev: fp(prev),
        prev2: fp(chat[index - 2]),
        target: (type === 'swipe' || type === 'continue') ? fp(target) : null,
        orig: type === 'continue' ? String(target?.mes ?? '') : null,
        base: type === 'continue' ? continueBase(target, sp) : null,
        replaced: type === 'regenerate' ? gen.replaced : null,
        userMsg: prev?.is_user && (type === 'normal' || type === 'regenerate') ? safe(() => structuredClone(prev)) : null,
    };
    return meta;
}

/** 与 Generate() 里的 continue_mag 一致：流式时直接取 StreamingProcessor 的值，否则按同样规则推算 */
function continueBase(target, sp) {
    if (sp && typeof sp.continueMessage === 'string' && sp.type === 'continue') return sp.continueMessage;
    let base = String(target?.mes ?? '');
    const postfix = OpenAI.oai_settings?.continue_postfix;
    if (ST.main_api === 'openai' && postfix && !base.endsWith(' ')) base += postfix;
    return base;
}

function safe(fn) {
    try { return fn(); } catch { return null; }
}

// ------------------------------------------------------------------ relay job (this page's own request)

class RelayJob {
    constructor(path, init, payload, meta) {
        this.id = randomId();
        this.path = path;
        this.init = init;
        this.payload = payload;
        this.meta = meta;
        this.signal = init.signal || null;
        this.state = 'starting'; // starting | streaming | finished | stopped | detached | lost | failed
        this.received = 0;
        this.reader = null;
        this.conn = null;
        this.ctrl = null;
        this.head = null;
        this.everConnected = false;
        this.accepted = false;
        this.statusFailures = 0;
        this.lastDataAt = Date.now();
        this.watchdog = null;
        this.unloadTimer = null;
        this.tagged = false;
        this.messageIndex = -1;
        this.doneMarked = false;
        this.acked = false;
    }

    reqHeaders() {
        const h = { ...headersObject() };
        const src = this.init.headers;
        if (src && typeof src === 'object' && !(src instanceof Headers)) Object.assign(h, src);
        else if (src instanceof Headers) src.forEach((v, k) => { h[k] = v; });
        h['Content-Type'] = 'application/json';
        return h;
    }

    async start() {
        if (this.signal?.aborted) throw abortError();
        this.signal?.addEventListener('abort', () => this.onSignalAbort(), { once: true });
        activeJobs.set(this.id, this);
        let res;
        try {
            res = await this.open(true);
        } catch (error) {
            activeJobs.delete(this.id);
            throw error;
        }
        if (res === 'fallback') {
            activeJobs.delete(this.id);
            return nativeFetch(this.path, this.init);
        }
        const status = Number(res.headers.get('X-BG-Status')) || 200;
        const statusText = safe(() => decodeURIComponent(res.headers.get('X-BG-Status-Text') || ''))?.replace(/[^\t\x20-\x7e]/g, '') ?? '';
        const contentType = safe(() => decodeURIComponent(res.headers.get('X-BG-Content-Type') || '')) || '';
        this.head = { status, statusText, contentType };
        this.reader = res.body.getReader();
        this.state = 'streaming';
        this.startWatchdog();
        const nullBody = [101, 204, 205, 304].includes(status);
        const body = nullBody ? null : new ReadableStream({
            start: (c) => { this.ctrl = c; },
            pull: (c) => this.pull(c),
            cancel: () => this.stop(),
        }, { highWaterMark: 0 });
        if (nullBody) this.drainQuietly();
        const headers = new Headers();
        if (contentType) headers.set('Content-Type', contentType);
        return new Response(body, { status: status < 200 || status > 599 ? 502 : status, statusText, headers });
    }

    async open(initial) {
        const started = Date.now();
        for (let attempt = 0; ; attempt++) {
            if (this.state === 'stopped' || this.state === 'detached') throw abortError();
            const ac = new AbortController();
            this.conn = ac;
            try {
                const startJob = () => nativeFetch(`${API}/start`, {
                    method: 'POST',
                    headers: this.reqHeaders(),
                    body: JSON.stringify({ id: this.id, target: this.path, meta: this.meta, payload: this.payload }),
                    signal: ac.signal,
                    cache: 'no-store',
                });
                const resume = () => nativeFetch(`${API}/stream/${this.id}?offset=${this.received}&client=${clientId}`, {
                    headers: this.reqHeaders(),
                    signal: ac.signal,
                    cache: 'no-store',
                });
                let res;
                if (initial && attempt > 0) {
                    // 首次请求失败（断网、网关超时等）：任务可能已经在服务器上跑了，先尝试接上，免得重复上传整个请求
                    res = await resume();
                    if (res.status === 404) res = await startJob();
                } else {
                    res = initial ? await startJob() : await resume();
                }
                if (res.ok && res.headers.get('X-BG-Job')) {
                    this.everConnected = true;
                    return res;
                }
                if (initial && !this.everConnected && [404, 405, 501].includes(res.status)) {
                    pluginState = 'missing';
                    renderStatus();
                    return 'fallback';
                }
                if (res.status === 409) throw new LostError();
                if (!initial && res.status === 404) throw fatal('后台任务已不存在（服务器可能重启过）');
                if ([400, 401, 403, 413, 416].includes(res.status)) throw fatal(`后台生成中转返回 HTTP ${res.status}`);
                // 其他（5xx、网关错误等）：稍后重试
            } catch (error) {
                if (this.state === 'stopped' || this.state === 'detached') throw abortError();
                if (error instanceof LostError || error?.bgFatal) throw error;
                // 网络错误：重试
            }
            if (initial && !this.everConnected && !this.accepted && Date.now() - started > 90_000) {
                // 一直没连上：服务器已经收到任务时（非流式请求超过 Cloudflare 的 100 秒、手机切后台断网等）继续等，否则按网络错误处理
                const status = await this.fetchStatus();
                if (this.state === 'stopped' || this.state === 'detached') throw abortError();
                if (status?.state === 'gone') throw new TypeError('Failed to fetch');
                if (status) this.accepted = true;
                // 查询本身失败（网络还没恢复）：还不能下结论，多试几次再放弃。按次数而不是时间算，手机锁屏时计时器不走
                else if (++this.statusFailures >= 15) throw new TypeError('Failed to fetch');
            }
            if ((!initial || this.accepted) && Date.now() - this.lastDataAt > 30 * 60_000) throw fatal('网络长时间不可用。回复会在服务器上继续生成，回来后打开聊天即可看到。');
            await wake(Math.min(500 * 2 ** attempt, 8000));
        }
    }

    async pull(controller) {
        for (;;) {
            if (!['streaming'].includes(this.state)) return;
            let result = null;
            try {
                result = await this.reader.read();
            } catch {
                result = null; // 连接断了
            }
            if (this.state !== 'streaming') return;
            if (result?.value?.byteLength) {
                this.received += result.value.byteLength;
                this.lastDataAt = Date.now();
                controller.enqueue(result.value);
                return;
            }
            if (result?.done) {
                const status = await this.fetchStatus();
                if (this.state !== 'streaming') return;
                if (status?.owner && status.owner !== clientId) return this.lose(controller);
                if (status && status.state !== 'running' && this.received >= status.length) return this.complete(controller);
            }
            try {
                const res = await this.open(false);
                this.reader = res.body.getReader();
            } catch (error) {
                if (error instanceof LostError) return this.lose(controller);
                if (this.state !== 'streaming') return;
                return this.fail(controller, error);
            }
        }
    }

    async fetchStatus() {
        try {
            const res = await fetchWithTimeout(`${API}/status/${this.id}`, { headers: this.reqHeaders(), cache: 'no-store' });
            if (res.status === 404) return { state: 'gone', length: Infinity };
            return res.ok ? await res.json() : null;
        } catch {
            return null;
        }
    }

    startWatchdog() {
        this.stopWatchdog();
        this.watchdog = setInterval(async () => {
            if (this.state !== 'streaming' || Date.now() - this.lastDataAt < 20_000) return;
            const status = await this.fetchStatus();
            // 服务器上的数据比我们收到的多，说明当前连接已经「假死」：断开重连
            if (status && this.state === 'streaming' && (status.length > this.received || status.state !== 'running')) {
                try { this.conn?.abort(); } catch { /* ignore */ }
            }
        }, 10_000);
    }

    stopWatchdog() {
        clearInterval(this.watchdog);
        this.watchdog = null;
    }

    nudge() {
        // 页面回到前台 / 网络恢复：如果一段时间没收到数据，立刻检查一次
        if (this.state === 'streaming' && Date.now() - this.lastDataAt > 3000) {
            this.fetchStatus().then(status => {
                if (status && this.state === 'streaming' && (status.length > this.received || status.state !== 'running')) {
                    try { this.conn?.abort(); } catch { /* ignore */ }
                }
            });
        }
    }

    complete(controller) {
        this.state = 'finished';
        this.stopWatchdog();
        try { controller.close(); } catch { /* ignore */ }
        // 辅助请求、或者错误响应（页面会像原版一样直接提示错误）：不需要事后补写
        if (this.meta.kind !== 'main' || this.head?.status >= 400) {
            this.ack();
            return;
        }
        setTimeout(() => this.ack(), ACK_FALLBACK_MS);
    }

    /** 给正在生成的消息打标记，便于页面意外关闭后识别「半截」消息 */
    tag(index) {
        const msg = ST.chat[index];
        if (!msg) return;
        msg.extra ??= {};
        msg.extra.bg_gen = { id: this.id, done: false };
        this.tagged = true;
        this.messageIndex = index;
    }

    /** SillyTavern 已经完整处理这条回复（MESSAGE_RECEIVED 已触发，文本是最终版本） */
    markDone() {
        const msg = ST.chat[this.messageIndex];
        if (msg?.extra?.bg_gen?.id === this.id) msg.extra.bg_gen.done = true;
        this.doneMarked = true;
    }

    ack() {
        if (this.acked) return;
        this.acked = true;
        activeJobs.delete(this.id);
        api(`/ack/${this.id}`, { method: 'POST' }).catch(() => { /* ignore */ });
    }

    onSignalAbort() {
        if (this.state !== 'streaming' && this.state !== 'starting') return;
        setTimeout(() => {
            if (this.state !== 'streaming' && this.state !== 'starting') return;
            if (Date.now() - lastBeforeUnload < 2000) {
                // 页面正在关闭 / 刷新：服务器继续生成。若页面最终没关（取消了离开），转为后台任务稍后补写
                clearTimeout(this.unloadTimer);
                this.unloadTimer = setTimeout(() => { if (!pageHidden) this.detach(); }, 5000);
                return;
            }
            this.stop();
        }, 0);
    }

    /** 用户点了停止 */
    stop() {
        if (!['streaming', 'starting'].includes(this.state)) return;
        this.state = 'stopped';
        this.stopWatchdog();
        try { this.conn?.abort(); } catch { /* ignore */ }
        api(`/abort/${this.id}`, { method: 'POST', keepalive: true }).catch(() => { /* ignore */ });
        try { this.ctrl?.error(abortError()); } catch { /* ignore */ }
        activeJobs.delete(this.id);
    }

    /** 页面里的 SillyTavern 已经不再接收（例如页面被冻结后恢复）：交给后台任务流程稍后补写 */
    detach() {
        if (!['streaming', 'starting'].includes(this.state)) return;
        this.state = 'detached';
        this.stopWatchdog();
        try { this.conn?.abort(); } catch { /* ignore */ }
        try { this.ctrl?.error(abortError()); } catch { /* ignore */ }
        activeJobs.delete(this.id);
        if (this.meta.kind === 'main') {
            scheduleCheck(3000);
        } else {
            api(`/abort/${this.id}`, { method: 'POST', keepalive: true }).catch(() => { /* ignore */ });
        }
    }

    lose(controller) {
        this.state = 'lost';
        this.stopWatchdog();
        const main = this.meta.kind === 'main';
        console.info(LOG, 'job taken over by another page', this.id);
        // 这一页（比如锁屏后醒来的手机）的聊天已经过期：先挡住它的保存，再从服务器重新加载
        if (main) saveQuarantineUntil = Date.now() + 15_000;
        try { controller.error(new LostError()); } catch { /* ignore */ }
        activeJobs.delete(this.id);
        if (main) {
            toastr.info('这条回复已在另一个页面写入，正在同步…', '后台生成');
            const ref = this.meta.chat;
            setTimeout(async () => {
                try {
                    console.info(LOG, 'reloading stale chat', sameChat(ref));
                    if (sameChat(ref)) await ST.reloadCurrentChat?.();
                } catch (error) {
                    console.warn(LOG, 'reload failed', error);
                } finally {
                    saveQuarantineUntil = 0;
                }
            }, 600);
        }
    }

    fail(controller, error) {
        this.state = 'failed';
        this.stopWatchdog();
        try { controller.error(error); } catch { /* ignore */ }
        activeJobs.delete(this.id);
        if (this.meta.kind === 'main') scheduleCheck(15_000);
    }

    async drainQuietly() {
        try { while (!(await this.reader.read()).done) { /* discard */ } } catch { /* ignore */ }
        this.state = 'finished';
        this.stopWatchdog();
        this.ack();
    }
}

// ------------------------------------------------------------------ fetch wrapper

function shouldRelay(info, init) {
    return settings.enabled
        && pluginState === 'ok'
        && info.method === 'POST'
        && TARGETS.has(info.path)
        && typeof init?.body === 'string';
}

async function relayFetch(path, init) {
    let payload;
    try {
        payload = JSON.parse(init.body);
    } catch {
        return nativeFetch(path, init);
    }
    if (!payload || typeof payload !== 'object') return nativeFetch(path, init);
    const meta = buildMeta(path, payload);
    const job = new RelayJob(path, init, payload, meta);
    return job.start();
}

if (!window.__bgGenFetchInstalled) {
    window.__bgGenFetchInstalled = true;
    window.fetch = function bgGenFetch(input, init) {
        try {
            const info = requestInfo(input, init);
            if (info) {
                if (replayNext && info.path === replayNext.path) {
                    const replay = replayNext;
                    replayNext = null;
                    return Promise.resolve(replay.makeResponse());
                }
                if (SAVE_PATHS.has(info.path) && info.method === 'POST') {
                    if (Date.now() < saveQuarantineUntil) {
                        console.info(LOG, 'skipped a stale chat save while reloading');
                        return Promise.resolve(new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } }));
                    }
                    return nativeFetch.apply(window, arguments).then(res => {
                        if (res.ok) onChatSaved();
                        return res;
                    });
                }
                if (shouldRelay(info, init)) return relayFetch(info.path, init);
            }
        } catch (error) {
            console.warn(LOG, 'relay skipped', error);
        }
        return nativeFetch.apply(window, arguments);
    };
}

function onChatSaved() {
    for (const job of activeJobs.values()) {
        if (job.meta.kind === 'main' && job.doneMarked) job.ack();
    }
}

// ------------------------------------------------------------------ parse cached results

async function parseResult(meta, head, bytes) {
    const text = new TextDecoder().decode(bytes);
    if (head.status >= 400) {
        let message = text.slice(0, 500);
        try {
            const data = JSON.parse(text);
            message = data?.error?.message || data?.message || JSON.stringify(data?.error ?? data).slice(0, 500);
        } catch { /* keep text */ }
        return { error: message || `HTTP ${head.status}` };
    }
    if (meta.api === 'openai') {
        if (meta.stream) return parseOpenAIStream(meta, bytes);
        const data = JSON.parse(text);
        if (data?.error) return { error: data.error.message || JSON.stringify(data.error) };
        return {
            text: ST.extractMessageFromData(data, 'openai') ?? '',
            reasoning: meta.showThoughts ? (Reasoning.extractReasoningFromData?.(data, { mainApi: 'openai', chatCompletionSource: meta.source, ignoreShowThoughts: true }) ?? '') : '',
            signature: safe(() => Reasoning.extractReasoningSignatureFromData(data, { mainApi: 'openai', chatCompletionSource: meta.source })) ?? null,
        };
    }
    if (meta.stream) return replayStream(meta, bytes);
    const data = JSON.parse(text);
    if (data?.error) return { error: data.error.message || JSON.stringify(data.error) };
    return {
        text: ST.extractMessageFromData(data, meta.api) ?? '',
        reasoning: Reasoning.extractReasoningFromData?.(data, { mainApi: meta.api, textGenType: meta.textgenType, ignoreShowThoughts: true }) ?? '',
    };
}

async function parseOpenAIStream(meta, bytes) {
    const es = new EventSourceStream();
    const writer = es.writable.getWriter();
    writer.write(bytes).catch(() => { /* ignore */ });
    writer.write(new TextEncoder().encode('\n\n')).catch(() => { /* ignore */ });
    writer.close().catch(() => { /* ignore */ });
    const reader = es.readable.getReader();
    const state = { reasoning: '', images: [], signature: '', toolSignatures: {} };
    let text = '';
    const swipes = [];
    for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        const raw = value?.data;
        if (raw === '[DONE]') break;
        let parsed;
        try { parsed = JSON.parse(raw); } catch { continue; }
        if (parsed?.error) return { error: parsed.error.message || JSON.stringify(parsed.error) };
        if (meta.multiSwipe && Array.isArray(parsed?.choices) && parsed.choices[0]?.index > 0) {
            const i = parsed.choices[0].index - 1;
            swipes[i] = (swipes[i] || '') + OpenAI.getStreamingReply(parsed, state, { chatCompletionSource: meta.source, overrideShowThoughts: false });
        } else {
            text += OpenAI.getStreamingReply(parsed, state, { chatCompletionSource: meta.source, overrideShowThoughts: meta.showThoughts });
        }
    }
    return { text, reasoning: state.reasoning || '', swipes: swipes.filter(x => typeof x === 'string'), images: state.images || [], signature: state.signature || null };
}

/** 文本补全 / Kobold / NovelAI：让 SillyTavern 自己的流式解析函数读一遍缓存的结果 */
async function replayStream(meta, bytes) {
    const fn = meta.api === 'textgenerationwebui' ? Textgen.generateTextGenWithStreaming
        : meta.api === 'kobold' ? Kobold.generateKoboldWithStreaming
            : meta.api === 'novel' ? Novel.generateNovelWithStreaming : null;
    if (typeof fn !== 'function') return { error: `暂不支持在页面关闭后补写 ${meta.api} 的结果` };
    replayNext = {
        path: meta.path,
        makeResponse: () => new Response(new Blob([bytes]).stream(), { status: 200 }),
    };
    try {
        const generator = await fn({}, new AbortController().signal);
        let last = null;
        for await (const item of generator()) last = item;
        return { text: last?.text ?? '', reasoning: last?.state?.reasoning ?? '', swipes: last?.swipes ?? [] };
    } finally {
        replayNext = null;
    }
}

// ------------------------------------------------------------------ apply results (page was closed)

function findTagged(id, from) {
    const chat = ST.chat;
    for (let i = chat.length - 1; i >= Math.max(0, from - 1); i--) {
        const m = chat[i];
        if (m?.extra?.bg_gen?.id === id) return { i, k: m.swipe_id ?? 0, where: 'mes', done: !!m.extra.bg_gen.done };
        if (Array.isArray(m?.swipe_info)) {
            const k = m.swipe_info.findIndex(si => si?.extra?.bg_gen?.id === id);
            if (k >= 0) return { i, k, where: 'swipe', done: !!m.swipe_info[k].extra.bg_gen.done };
        }
    }
    return null;
}

function restoreSwipeState(m, keep) {
    m.swipes.length = keep;
    if (Array.isArray(m.swipe_info)) m.swipe_info.length = Math.min(m.swipe_info.length, keep);
    m.swipe_id = Math.max(0, keep - 1);
    m.mes = m.swipes[m.swipe_id] ?? '';
    const info = m.swipe_info?.[m.swipe_id];
    if (info) {
        m.send_date = info.send_date;
        m.gen_started = info.gen_started;
        m.gen_finished = info.gen_finished;
        m.extra = structuredClone(info.extra ?? {});
    }
}

/**
 * 根据当前聊天状态决定怎么写入。返回 { action: 'apply'|'already'|'conflict', mode, fixes, reason }
 */
function reconcile(job) {
    const meta = job.meta;
    const a = meta.anchor || {};
    const chat = ST.chat;
    const type = meta.type;
    const mode = type === 'swipe' ? 'swipe' : type === 'continue' ? 'continue' : 'normal';
    const conflict = reason => ({ action: 'conflict', mode, reason });
    const tagged = findTagged(job.id, a.index ?? chat.length);
    if (tagged?.done) return { action: 'already' };
    const fixes = [];

    if (mode === 'normal') {
        let len = chat.length;
        if (tagged) {
            if (tagged.i !== chat.length - 1) return conflict('这条回复之后已经有新消息');
            fixes.push(() => ST.deleteLastMessage());
            len -= 1;
        } else if (type === 'regenerate' && len === a.index + 1 && fpMatch(chat[a.index], a.replaced)) {
            fixes.push(() => ST.deleteLastMessage());
            len -= 1;
        }
        if (len === a.index) {
            if (!fpMatch(chat[a.index - 1], a.prev)) return conflict('聊天内容已变化');
        } else if (len === a.index - 1 && a.userMsg && fpMatch(chat[a.index - 2], a.prev2)) {
            // 用户消息还没来得及保存页面就关了：补回去
            fixes.push(() => {
                const msg = structuredClone(a.userMsg);
                chat.push(msg);
                ST.addOneMessage(msg);
            });
        } else {
            return conflict('聊天内容已变化');
        }
        return { action: 'apply', mode, fixes };
    }

    const t = a.index;
    const m = chat[t];
    if (chat.length !== t + 1 || !m || m.is_user) return conflict('目标消息已不是最后一条');

    if (mode === 'swipe') {
        if (!Array.isArray(m.swipes) || !fpMatch(m, a.target)) return conflict('目标消息已变化');
        if (tagged && tagged.i === t) {
            if (tagged.k !== m.swipes.length - 1) return conflict('目标消息的滑动记录已变化');
            fixes.push(() => restoreSwipeState(m, m.swipes.length - 1));
        } else if (m.swipe_id >= m.swipes.length) {
            fixes.push(() => restoreSwipeState(m, m.swipes.length));
        }
        return { action: 'apply', mode, fixes };
    }

    // continue：页面关闭前可能已经存了「原文 + 半截续写」，先恢复原文
    const orig = a.orig ?? '';
    const ours = tagged && tagged.i === t;
    if (!ours && !fpMatch(m, a.target) && !(m.name === a.target?.n && String(m.mes).startsWith(orig))) return conflict('目标消息已变化');
    if (!String(m.mes).startsWith(orig.trimEnd())) return conflict('目标消息内容已被修改');
    fixes.push(() => {
        m.mes = orig;
        if (Array.isArray(m.swipes) && m.swipe_id !== undefined) m.swipes[m.swipe_id] = orig;
    });
    return { action: 'apply', mode, fixes };
}

async function withSpeaker(meta, fn) {
    if (!meta.chat?.group || !meta.speaker?.avatar) return fn();
    const chid = ST.characters.findIndex(c => c.avatar === meta.speaker.avatar);
    if (chid < 0) return fn();
    const prevChid = ST.this_chid;
    const prevName = ST.name2;
    ST.setCharacterId(chid);
    ST.setCharacterName(meta.speaker.name || ST.characters[chid].name);
    try {
        return await fn();
    } finally {
        ST.setCharacterId(prevChid);
        ST.setCharacterName(prevName);
    }
}

/** 走 SillyTavern 非流式生成的收尾流程（与 Generate() 的 onSuccess 一致） */
async function finalizeInto(job, parsed, mode) {
    const meta = job.meta;
    const isContinue = mode === 'continue';
    await withSpeaker(meta, async () => {
        let getMessage = String(parsed.text ?? '');
        let reasoningText = String(parsed.reasoning ?? '');
        if (typeof Regex.getRegexedString === 'function' && Regex.regex_placement) {
            reasoningText = Regex.getRegexedString(reasoningText, Regex.regex_placement.REASONING);
        }
        if (power_user.trim_spaces) reasoningText = reasoningText.trim();
        if (isContinue) getMessage = (meta.anchor?.base ?? '') + getMessage;
        getMessage = ST.cleanUpMessage({ getMessage, isImpersonate: false, isContinue, displayIncompleteSentences: false });
        if (mode === 'swipe') {
            const last = ST.chat[ST.chat.length - 1];
            last.swipe_id = last.swipes.length;
        }
        await ST.saveReply({
            type: isContinue ? 'appendFinal' : mode === 'swipe' ? 'swipe' : 'normal',
            getMessage,
            title: '',
            swipes: Array.isArray(parsed.swipes) ? parsed.swipes : [],
            reasoning: reasoningText,
            imageUrls: Array.isArray(parsed.images) ? parsed.images : [],
            reasoningSignature: parsed.signature || null,
        });
    });

    const idx = ST.chat.length - 1;
    const msg = ST.chat[idx];
    if (!msg) return;
    msg.extra ??= {};
    msg.extra.bg_gen = { id: job.id, done: true };
    if (meta.genApi) msg.extra.api = meta.genApi;
    if (meta.genModel) msg.extra.model = meta.genModel;
    if (meta.startedAt) msg.gen_started = new Date(meta.startedAt);
    if (job.finishedAt) msg.gen_finished = new Date(job.finishedAt);
    if (job.headAt || job.finishedAt) msg.send_date = new Date(job.headAt || job.finishedAt).toISOString();
    if (meta.chat?.group && meta.speaker?.genId) msg.extra.gen_id = meta.speaker.genId;
    safe(() => ST.syncMesToSwipe(idx));
    safe(() => ST.updateMessageBlock(idx, msg));
    await ST.saveChatConditional();
}

async function fetchJobBytes(id) {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 120_000);
    try {
        const res = await nativeFetch(`${API}/stream/${id}?offset=0&client=${clientId}`, { headers: headersObject(), cache: 'no-store', signal: ac.signal });
        if (res.status === 409) throw new LostError();
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const head = {
            status: Number(res.headers.get('X-BG-Status')) || 200,
            contentType: safe(() => decodeURIComponent(res.headers.get('X-BG-Content-Type') || '')) || '',
        };
        const bytes = new Uint8Array(await res.arrayBuffer());
        return { head, bytes };
    } finally {
        clearTimeout(timer);
    }
}

async function applyJob(job, { partial = null } = {}) {
    const meta = job.meta;
    let parsed;
    try {
        if (partial) {
            parsed = partial;
        } else {
            const { head, bytes } = await fetchJobBytes(job.id);
            parsed = await parseResult(meta, head, bytes);
        }
    } catch (error) {
        if (error instanceof LostError) return;
        console.warn(LOG, 'parse failed', error);
        parsed = { error: error.message };
    }

    if (parsed.error) {
        toastr.error(`「${meta.chat?.label ?? ''}」的后台生成失败：${parsed.error}`, '后台生成', { timeOut: 15000 });
        await ackJob(job.id);
        return;
    }
    if (!parsed.text && !parsed.reasoning) {
        toastr.warning(`「${meta.chat?.label ?? ''}」的后台生成没有返回内容。`, '后台生成');
        await ackJob(job.id);
        return;
    }

    if (!sameChat(meta.chat) || isGenerating()) {
        scheduleCheck(2000);
        return;
    }

    const plan = reconcile(job);
    if (plan.action === 'already') {
        await ackJob(job.id);
        return;
    }
    if (plan.action === 'conflict') {
        await resolveConflict(job, parsed, plan.reason);
        return;
    }
    for (const fix of plan.fixes) await fix();
    await finalizeInto(job, parsed, plan.mode);
    await ackJob(job.id);
    if (settings.notify) toastr.success('页面关闭期间生成的回复已补上。', '后台生成', { timeOut: 3000 });
}

async function resolveConflict(job, parsed, reason) {
    deferred.add(job.id);
    const preview = String(parsed.text ?? '').slice(0, 1500);
    const html = `
        <div class="bggen-conflict">
            <h3>后台生成的回复已完成</h3>
            <p>不过${esc(reason)}，无法自动放回原位置。</p>
            <pre>${esc(preview)}${parsed.text?.length > 1500 ? '\n…' : ''}</pre>
        </div>`;
    const popup = new Popup(html, POPUP_TYPE.TEXT, '', {
        okButton: '追加到聊天末尾',
        cancelButton: '以后再说',
        customButtons: [{ text: '丢弃', result: POPUP_RESULT.CUSTOM1 ?? 1001 }],
        wide: true,
    });
    const result = await popup.show();
    if (result === POPUP_RESULT.AFFIRMATIVE) {
        if (!sameChat(job.meta.chat) || isGenerating()) {
            toastr.warning('当前不在该聊天或正在生成，稍后再试。', '后台生成');
            deferred.delete(job.id);
            return;
        }
        await finalizeInto(job, parsed, 'normal');
        await ackJob(job.id);
    } else if (result === (POPUP_RESULT.CUSTOM1 ?? 1001)) {
        await ackJob(job.id);
    }
}

async function ackJob(id) {
    settled.add(id);
    try { await api(`/ack/${id}`, { method: 'POST' }); } catch { /* ignore */ }
}

// ------------------------------------------------------------------ pending-job checks

function scheduleCheck(ms = 300) {
    clearTimeout(recheckTimer);
    recheckTimer = setTimeout(checkPendingJobs, ms);
}

async function checkPendingJobs() {
    if (pluginState !== 'ok' || !settings.enabled) return;
    if (checking) {
        scheduleCheck(1000);
        return;
    }
    checking = true;
    let again = 0;
    try {
        const res = await api('/jobs');
        if (!res.ok) {
            again = 10_000;
            return;
        }
        lastJobs = res.data?.jobs ?? [];
        renderJobs();
        for (const job of lastJobs) {
            if (reattach) break;
            if (activeJobs.has(job.id) || settled.has(job.id) || deferred.has(job.id)) continue;
            if (!sameChat(job.meta?.chat)) continue;
            if (isGenerating()) {
                again = 2000;
                continue;
            }
            const attachedElsewhere = job.attached > 0 && job.owner !== clientId;
            if (job.state === 'running') {
                if (attachedElsewhere) {
                    watchElsewhere(job);
                    break;
                }
                startReattach(job);
                break;
            }
            if (attachedElsewhere && Date.now() - (job.finishedAt || 0) < DONE_GRACE_MS) {
                again = 5000;
                continue;
            }
            const claim = await api(`/claim/${job.id}`, { method: 'POST', body: { client: clientId } });
            if (!claim.ok) {
                if (claim.status === 409) again = 5000;
                continue;
            }
            await applyJob(job);
        }
        notifyOtherChats();
    } catch (error) {
        console.debug(LOG, 'check failed', error);
        again = 10_000;
    } finally {
        checking = false;
        if (again) scheduleCheck(again);
    }
}

const notified = new Set(safe(() => JSON.parse(sessionStorage.getItem('bggen-notified') || '[]')) || []);

function notifyOtherChats() {
    if (!settings.notify || !notifyReady) return;
    const others = lastJobs.filter(j => j.state === 'done' && (j.head?.status ?? 200) < 400
        && !sameChat(j.meta?.chat) && !notified.has(j.id) && !settled.has(j.id));
    for (const job of others) {
        notified.add(job.id);
        toastr.info(`「${job.meta?.chat?.label ?? '某个聊天'}」有一条后台生成的回复，打开该聊天即可看到。`, '后台生成', { timeOut: 8000 });
    }
    safe(() => sessionStorage.setItem('bggen-notified', JSON.stringify([...notified].slice(-100))));
}

// ------------------------------------------------------------------ re-attach to a running job (page was reopened mid-generation)

function blockKeys(event) {
    if (!reattach) return;
    const inInput = event.target?.id === 'send_textarea';
    if (event.key === 'Enter' && !event.isComposing && (inInput || event.ctrlKey || event.altKey) && !event.shiftKey) {
        event.preventDefault();
        event.stopImmediatePropagation();
        toastr.info('后台回复还在生成，请稍候，或点停止。', '后台生成', { preventDuplicates: true });
    }
}

async function startReattach(job) {
    const claim = await api(`/claim/${job.id}`, { method: 'POST', body: { client: clientId } });
    if (!claim.ok) {
        scheduleCheck(5000);
        return;
    }
    const state = { id: job.id, job, received: 0, chunks: [], stopped: false, conn: null, text: '' };
    reattach = state;
    console.info(LOG, 're-attaching to a running background job', job.id);
    safe(() => ST.deactivateSendButtons());
    showIndicator(`${job.meta?.speaker?.name || job.meta?.chat?.label || ''} 的回复仍在后台生成…`);

    let head = { status: 200 };
    const started = Date.now();
    let lastData = Date.now();
    // 和本页自己的请求一样：长时间没数据时检查一下连接是不是「假死」
    const watchdog = setInterval(async () => {
        if (state.stopped || Date.now() - lastData < 20_000) return;
        const st = await api(`/status/${job.id}`).catch(() => null);
        if (st?.data && (st.data.length > state.received || st.data.state !== 'running')) {
            try { state.conn?.abort(); } catch { /* ignore */ }
        }
    }, 10_000);
    try {
        for (let attempt = 0; !state.stopped; attempt++) {
            const ac = new AbortController();
            state.conn = ac;
            let res;
            try {
                res = await nativeFetch(`${API}/stream/${job.id}?offset=${state.received}&client=${clientId}`, { headers: headersObject(), signal: ac.signal, cache: 'no-store' });
            } catch {
                if (state.stopped) break;
                await wake(Math.min(500 * 2 ** attempt, 8000));
                continue;
            }
            if (res.status === 409 || res.status === 404) throw new LostError();
            if (!res.ok) {
                await wake(Math.min(500 * 2 ** attempt, 8000));
                continue;
            }
            head = { status: Number(res.headers.get('X-BG-Status')) || 200 };
            const reader = res.body.getReader();
            try {
                for (;;) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    if (value?.byteLength) {
                        state.chunks.push(value);
                        state.received += value.byteLength;
                        lastData = Date.now();
                        updateIndicator(state, started);
                    }
                }
            } catch {
                if (state.stopped) break;
            }
            if (state.stopped) break;
            const status = await api(`/status/${job.id}`).catch(() => null);
            if (status?.data && status.data.state !== 'running' && state.received >= status.data.length) {
                job.finishedAt = status.data.finishedAt;
                job.headAt = status.data.headAt;
                break;
            }
            if (Date.now() - lastData > 30 * 60_000) throw new Error('网络长时间不可用');
            await wake(1000);
        }
        clearInterval(watchdog);
        hideIndicator();
        reattach = null;
        safe(() => ST.activateSendButtons());
        const bytes = concatChunks(state.chunks);
        if (state.stopped) {
            // 用户点了停止：和原版一样保留已生成的部分
            const parsed = await parseResult(job.meta, head, bytes).catch(e => ({ error: e.message }));
            await applyJob(job, { partial: parsed.error ? { text: '', error: parsed.error } : parsed });
        } else {
            await applyJob(job);
        }
    } catch (error) {
        clearInterval(watchdog);
        hideIndicator();
        reattach = null;
        safe(() => ST.activateSendButtons());
        if (!(error instanceof LostError)) {
            console.warn(LOG, 'reattach failed', error);
            scheduleCheck(10_000);
        }
    }
}

/**
 * 这条回复正由另一个页面（另一台设备 / 另一个标签页）接收：本页先锁住发送，
 * 等那边写完后从服务器重新加载聊天，避免本页的旧内容覆盖那边的结果。
 * 如果那边中途消失了，就由本页接手。
 */
async function watchElsewhere(job) {
    if (reattach) return;
    const state = { id: job.id, watching: true, stopped: false };
    reattach = state;
    console.info(LOG, 'another page is receiving this reply; watching', job.id);
    const chatRef = job.meta?.chat;
    const epoch = chatEpoch;
    safe(() => ST.deactivateSendButtons());
    showIndicator('另一个页面正在接收这条回复，完成后自动显示…');
    let outcome = 'reload';
    try {
        for (;;) {
            await wake(3000);
            if (state.stopped) break;
            if (chatEpoch !== epoch && !sameChat(chatRef)) {
                outcome = 'none';
                break;
            }
            const res = await api(`/status/${job.id}`).catch(() => null);
            if (!res) continue;
            if (res.status === 404 || res.data?.acked) break; // 那边已经写完并确认
            if (!res.ok) continue;
            const st = res.data;
            const el = document.querySelector('#bggen_indicator .bggen-count');
            if (el) el.textContent = `${(st.length / 1024).toFixed(1)} KB`;
            const ownerGone = st.attached === 0 && st.owner !== clientId;
            const doneLongAgo = st.state !== 'running' && Date.now() - (st.finishedAt || 0) > DONE_GRACE_MS;
            if (ownerGone || doneLongAgo) {
                outcome = 'takeover';
                break;
            }
        }
    } finally {
        hideIndicator();
        reattach = null;
        safe(() => ST.activateSendButtons());
    }
    if (state.stopped) {
        // 用户在本页点了停止：中止任务，稍后同步那边留下的部分内容
        setTimeout(() => { if (sameChat(chatRef) && !isGenerating()) ST.reloadCurrentChat?.(); }, 2500);
    } else if (outcome === 'reload') {
        if (sameChat(chatRef) && !isGenerating()) await ST.reloadCurrentChat?.();
    } else if (outcome === 'takeover') {
        scheduleCheck(0);
    }
}

function stopReattach() {
    if (!reattach || reattach.stopped) return;
    reattach.stopped = true;
    try { reattach.conn?.abort(); } catch { /* ignore */ }
    api(`/abort/${reattach.id}`, { method: 'POST' }).catch(() => { /* ignore */ });
    if (reattach.watching) wakeAll();
}

function concatChunks(chunks) {
    const total = chunks.reduce((s, c) => s + c.byteLength, 0);
    const out = new Uint8Array(total);
    let pos = 0;
    for (const c of chunks) {
        out.set(c, pos);
        pos += c.byteLength;
    }
    return out;
}

function showIndicator(text) {
    let el = document.getElementById('bggen_indicator');
    if (!el) {
        el = document.createElement('div');
        el.id = 'bggen_indicator';
        el.innerHTML = '<i class="fa-solid fa-cloud-arrow-down fa-fade"></i><span class="bggen-text"></span><span class="bggen-count"></span><div class="menu_button bggen-stop">停止</div>';
        el.querySelector('.bggen-stop').addEventListener('click', stopReattach);
        const anchor = document.getElementById('form_sheld') || document.body;
        anchor.parentElement?.insertBefore(el, anchor) ?? document.body.appendChild(el);
    }
    el.querySelector('.bggen-text').textContent = text;
    el.querySelector('.bggen-count').textContent = '';
}

function updateIndicator(state, started) {
    const el = document.getElementById('bggen_indicator');
    if (!el) return;
    const secs = Math.round((Date.now() - started) / 1000);
    el.querySelector('.bggen-count').textContent = `${(state.received / 1024).toFixed(1)} KB · ${secs}s`;
}

function hideIndicator() {
    document.getElementById('bggen_indicator')?.remove();
}

// ------------------------------------------------------------------ settings panel

function renderStatus() {
    const el = document.getElementById('bggen_status');
    if (!el) return;
    const map = {
        ok: ['is-good', '服务端插件已连接，关闭网页后回复会继续生成。'],
        missing: ['is-bad', '没有检测到服务端插件 SillyTavern-BackgroundGen-Server，当前按原版方式生成。'],
        unknown: ['', '正在检测服务端插件…'],
    };
    const [cls, text] = map[pluginState] ?? map.unknown;
    el.className = `bggen-status ${cls}`;
    el.textContent = text;
}

function renderJobs() {
    const el = document.getElementById('bggen_jobs');
    if (!el) return;
    if (!lastJobs.length) {
        el.innerHTML = '<div class="bggen-muted">没有待处理的后台回复。</div>';
        return;
    }
    const stateText = { running: '生成中', done: '已完成，待写入', error: '失败' };
    el.innerHTML = lastJobs.map(j => `
        <div class="bggen-job" data-id="${esc(j.id)}">
            <div><b>${esc(j.meta?.chat?.label ?? '?')}</b> <span class="bggen-muted">${esc(j.meta?.type ?? '')} · ${new Date(j.createdAt).toLocaleString()}</span></div>
            <div class="bggen-muted">${esc(stateText[j.state] ?? j.state)} · ${(j.length / 1024).toFixed(1)} KB${sameChat(j.meta?.chat) ? ' · 当前聊天' : ''}</div>
            <div class="menu_button bggen-discard" title="丢弃这条后台结果">丢弃</div>
        </div>`).join('');
    el.querySelectorAll('.bggen-discard').forEach(btn => btn.addEventListener('click', async (e) => {
        const id = e.target.closest('.bggen-job')?.dataset.id;
        if (!id || !confirm('确定丢弃这条后台生成的结果吗？')) return;
        const job = lastJobs.find(j => j.id === id);
        await api(job?.state === 'running' ? `/abort/${id}` : `/ack/${id}`, { method: 'POST' });
        settled.add(id);
        await checkPendingJobs();
    }));
}

function addSettingsPanel() {
    const host = document.getElementById('extensions_settings2') || document.getElementById('extensions_settings');
    if (!host || document.getElementById('bggen_settings')) return;
    host.insertAdjacentHTML('beforeend', `
        <div id="bggen_settings" class="bggen-settings">
            <div class="inline-drawer">
                <div class="inline-drawer-toggle inline-drawer-header">
                    <b>后台生成</b>
                    <div class="inline-drawer-icon fa-solid fa-circle-chevron-down down"></div>
                </div>
                <div class="inline-drawer-content">
                    <div id="bggen_status" class="bggen-status"></div>
                    <label class="checkbox_label"><input type="checkbox" id="bggen_enabled"> 启用后台生成（关闭网页、锁屏、断网后继续生成）</label>
                    <label class="checkbox_label"><input type="checkbox" id="bggen_notify"> 补写回复时显示提示</label>
                    <h4>待处理的后台回复</h4>
                    <div id="bggen_jobs"></div>
                    <div class="menu_button" id="bggen_refresh">刷新</div>
                    <div class="bggen-muted bggen-ver">v${VERSION}</div>
                </div>
            </div>
        </div>`);
    const enabled = document.getElementById('bggen_enabled');
    const notify = document.getElementById('bggen_notify');
    enabled.checked = !!settings.enabled;
    notify.checked = !!settings.notify;
    enabled.addEventListener('change', () => { settings.enabled = enabled.checked; ST.saveSettingsDebounced(); });
    notify.addEventListener('change', () => { settings.notify = notify.checked; ST.saveSettingsDebounced(); });
    document.getElementById('bggen_refresh').addEventListener('click', () => checkPendingJobs());
    renderStatus();
    renderJobs();
}

// ------------------------------------------------------------------ events

function registerEvents() {
    const { eventSource, event_types } = ST;
    eventSource.on(event_types.GENERATION_STARTED, (type, _opts, dryRun) => {
        if (dryRun || !MAIN_TYPES.has(type)) return;
        const last = ST.chat[ST.chat.length - 1];
        mainGen = {
            type,
            at: Date.now(),
            consumed: false,
            replaced: type === 'regenerate' && last && !last.is_user ? fp(last) : null,
        };
    });

    const remember = (fn) => (typeof eventSource.makeLast === 'function'
        ? (ev) => eventSource.makeLast(ev, fn)
        : (ev) => eventSource.on(ev, fn));
    remember((data) => {
        lastPrepared = {
            at: Date.now(), api: 'openai', type: data?.type || 'normal',
            n: Array.isArray(data?.messages) ? data.messages.length : -1, stream: !!data?.stream,
        };
    })(event_types.CHAT_COMPLETION_SETTINGS_READY);
    remember((data) => {
        lastPrepared = { at: Date.now(), api: 'textgen', promptLength: String(data?.prompt ?? '').length };
    })(event_types.TEXT_COMPLETION_SETTINGS_READY);

    // 流式生成：占位消息出现后打标记
    eventSource.on(event_types.STREAM_TOKEN_RECEIVED, () => {
        const sp = ST.streamingProcessor;
        if (!sp || sp.messageId < 0) return;
        for (const job of activeJobs.values()) {
            if (job.meta.kind === 'main' && job.meta.stream && !job.tagged && ['streaming', 'finished'].includes(job.state)) {
                job.tag(sp.messageId);
                break;
            }
        }
    });

    // SillyTavern 收尾完成（文本已是最终版本）：标记完成，下一次保存后确认
    // 注意：SSE 读到 [DONE] 时 SillyTavern 就会收尾，此时我们的流可能还没关闭
    eventSource.on(event_types.MESSAGE_RECEIVED, (rawIndex) => {
        const index = Number(rawIndex);
        for (const job of activeJobs.values()) {
            if (job.meta.kind !== 'main' || job.doneMarked) continue;
            if (job.meta.stream) {
                if (job.state !== 'streaming' && job.state !== 'finished') continue;
                if (!job.tagged) {
                    if (ST.streamingProcessor?.messageId !== index) continue;
                    job.tag(index);
                }
                if (job.messageIndex !== index) continue;
            } else {
                if (job.state !== 'finished') continue;
                if (!job.tagged) job.tag(index);
            }
            job.markDone();
            break;
        }
    });

    eventSource.on(event_types.CHAT_CHANGED, () => {
        chatEpoch++;
        deferred.clear();
        setTimeout(() => { notifyReady = true; }, 3000);
        scheduleCheck(400);
    });
    eventSource.on(event_types.GENERATION_ENDED, () => { if (lastJobs.length) scheduleCheck(1500); });
    eventSource.on(event_types.GENERATION_STOPPED, () => stopReattach());

    window.addEventListener('beforeunload', () => { lastBeforeUnload = Date.now(); }, { capture: true });
    window.addEventListener('pagehide', () => { pageHidden = true; });
    window.addEventListener('pageshow', (event) => {
        pageHidden = false;
        if (event.persisted) {
            // 从往返缓存恢复：之前因卸载而停下的请求交给后台流程
            for (const job of activeJobs.values()) {
                if (job.signal?.aborted) job.detach();
            }
            wakeAll();
            scheduleCheck(1000);
        }
    });
    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState !== 'visible') return;
        wakeAll();
        for (const job of activeJobs.values()) job.nudge();
        scheduleCheck(800);
    });
    window.addEventListener('online', () => {
        wakeAll();
        for (const job of activeJobs.values()) job.nudge();
    });
    document.addEventListener('keydown', blockKeys, { capture: true });
}

async function detectPlugin() {
    for (let i = 0; i < 3; i++) {
        try {
            const res = await fetchWithTimeout(`${API}/health`, { headers: headersObject(), cache: 'no-store' }, 10_000);
            if (res.ok && (await res.json())?.ok) {
                pluginState = 'ok';
                renderStatus();
                return;
            }
            if (res.status === 404) break;
        } catch { /* retry */ }
        await sleep(1500);
    }
    pluginState = 'missing';
    renderStatus();
}

jQuery(async () => {
    try {
        registerEvents();
        addSettingsPanel();
        await detectPlugin();
        if (pluginState === 'ok') scheduleCheck(1500);
        // 兜底：已知有待处理的后台回复时，页面可见期间每 30 秒再查一次
        setInterval(() => {
            if (pluginState === 'ok' && lastJobs.length && document.visibilityState === 'visible' && !checking && !reattach) scheduleCheck(0);
        }, 30_000);
        setTimeout(() => { notifyReady = true; scheduleCheck(0); }, 10_000);
        console.info(`${LOG} v${VERSION} loaded, plugin: ${pluginState}`);
    } catch (error) {
        console.error(LOG, 'init failed', error);
    }
});
