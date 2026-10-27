// ==UserScript==
// @name         nxrunner WebsiteController
// @namespace    nxrunner
// @version      1.0
// @description  Steuerung der Webseite durch den nxrunner-Runner WebsiteController (__SERVER__)
// @match        *://*/*
// @noframes
// @grant        GM_xmlhttpRequest
// @connect      __HOST__
// @connect      localhost
// @connect      127.0.0.1
// @run-at       document-idle
// ==/UserScript==

(function () {
    'use strict';

    const SERVER = '__SERVER__';
    const CLIENT = Math.random().toString(36).slice(2, 10);
    const MARK = 'nx-website-mark';

    function request(path, data, timeout) {
        if (typeof GM_xmlhttpRequest === 'undefined') {
            // ohne Userscript-Manager (z.B. in die Konsole kopiert): fetch, der Server sendet CORS-Header
            const ctrl = new AbortController();
            const timer = setTimeout(function () { ctrl.abort(); }, timeout);
            return fetch(SERVER + path, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data),
                signal: ctrl.signal
            }).then(function (res) { return res.json(); }).finally(function () { clearTimeout(timer); });
        }
        return new Promise(function (resolve, reject) {
            GM_xmlhttpRequest({
                method: 'POST',
                url: SERVER + path,
                headers: {'Content-Type': 'application/json'},
                data: JSON.stringify(data),
                timeout: timeout,
                onload: function (res) {
                    try { resolve(JSON.parse(res.responseText)); } catch (e) { reject(e); }
                },
                onerror: function (res) { reject(new Error('Verbindung fehlgeschlagen ' + (res && res.error || ''))); },
                ontimeout: function () { reject(new Error('Timeout')); }
            });
        });
    }

    // --- Elemente ---------------------------------------------------------------------------

    function isInput(el) {
        return el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement || el instanceof HTMLSelectElement;
    }

    function readValue(el, attr) {
        if (el === null) return null;
        if (attr === 'html') return el.innerHTML;
        if (attr === 'text') return el.textContent.trim();
        if (attr) return attr === 'value' && isInput(el) ? el.value : el.getAttribute(attr);
        if (el instanceof HTMLInputElement && (el.type === 'checkbox' || el.type === 'radio')) return el.checked ? '1' : '0';
        if (isInput(el)) return el.value;
        return el.textContent.trim();
    }

    function writeValue(el, attr, value) {
        if (attr === 'html') { el.innerHTML = value; return; }
        if (attr === 'text') { el.textContent = value; return; }
        if (attr && !(attr === 'value' && isInput(el))) { el.setAttribute(attr, value); return; }
        if (el instanceof HTMLInputElement && (el.type === 'checkbox' || el.type === 'radio')) {
            el.checked = ['1', 'true', 'on', 'yes'].includes(String(value).toLowerCase());
        } else if (isInput(el)) {
            // Setter des Prototyps, damit Frameworks wie React die Aenderung mitbekommen
            const proto = Object.getPrototypeOf(el);
            const setter = Object.getOwnPropertyDescriptor(proto, 'value').set;
            setter.call(el, value);
        } else {
            el.textContent = value;
            return;
        }
        el.dispatchEvent(new Event('input', {bubbles: true}));
        el.dispatchEvent(new Event('change', {bubbles: true}));
    }

    function describe(el, i) {
        const attrs = {};
        for (const a of el.attributes) attrs[a.name] = a.value.length > 200 ? a.value.slice(0, 200) + '…' : a.value;
        const text = el.textContent.trim().replace(/\s+/g, ' ');
        return {
            i: i,
            tag: el.tagName.toLowerCase(),
            attrs: attrs,
            text: text.length > 300 ? text.slice(0, 300) + '…' : text,
            value: isInput(el) ? el.value : null,
            children: el.children.length
        };
    }

    // Ergebnis von JS als JSON-taugliche Struktur, Elemente wie bei query
    function serialize(v, depth) {
        if (v === undefined) return null;
        if (v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
        if (typeof v === 'function') return String(v);
        if (v instanceof Element) return describe(v, 0);
        if (depth > 3) return String(v);
        if (v instanceof NodeList || v instanceof HTMLCollection || Array.isArray(v)) {
            return Array.from(v).slice(0, 200).map(function (x, i) {
                return x instanceof Element ? describe(x, i) : serialize(x, depth + 1);
            });
        }
        if (v instanceof Node) return String(v.textContent);
        try {
            return JSON.parse(JSON.stringify(v));
        } catch (e) {
            const o = {};
            Object.keys(v).slice(0, 100).forEach(function (k) { o[k] = serialize(v[k], depth + 1); });
            return o;
        }
    }

    function field(base, f) {
        if (!f.selector) return base;
        // "input[1]": zweiter Treffer von "input" im Basis-Element, "[1]" allein: zweites Kind, negativ von hinten
        const m = f.selector.match(/^(.*?)\s*\[(-?\d+)\]$/);
        if (m) {
            const all = m[1] ? base.querySelectorAll(m[1]) : base.children;
            const i = Number(m[2]);
            return all[i < 0 ? all.length + i : i] || null;
        }
        return base.querySelector(f.selector);
    }

    // --- Befehle ----------------------------------------------------------------------------

    const COMMANDS = {
        query: function (c) {
            const els = Array.from(document.querySelectorAll(c.selector));
            const limit = c.limit || 200;
            return {count: els.length, items: els.slice(0, limit).map(describe)};
        },
        highlight: function (c) {
            document.querySelectorAll('.' + MARK).forEach(function (el) {
                el.classList.remove(MARK);
                el.style.outline = el.dataset.nxOutline || '';
                delete el.dataset.nxOutline;
            });
            if (!c.selector) return {count: 0};
            const els = Array.from(document.querySelectorAll(c.selector));
            els.forEach(function (el) {
                el.dataset.nxOutline = el.style.outline;
                el.style.outline = '2px solid #e5197d';
                el.classList.add(MARK);
            });
            if (els.length > 0) els[0].scrollIntoView({block: 'center', behavior: 'smooth'});
            return {count: els.length};
        },
        extract: function (c) {
            const bases = Array.from(document.querySelectorAll(c.base));
            const rows = bases.map(function (b) {
                const row = {};
                c.fields.forEach(function (f) { row[f.name] = readValue(field(b, f), f.attr); });
                return row;
            });
            return {count: rows.length, columns: c.fields.map(function (f) { return f.name; }), rows: rows};
        },
        apply: function (c) {
            const bases = Array.from(document.querySelectorAll(c.base));
            const byName = {};
            c.fields.forEach(function (f) { byName[f.name] = f; });
            const errors = [];
            let applied = 0;
            c.changes.forEach(function (ch) {
                const b = bases[ch.row];
                const f = byName[ch.field];
                const el = b && f ? field(b, f) : null;
                if (el === null) {
                    errors.push('Zeile ' + ch.row + ', ' + ch.field + ': Element nicht gefunden');
                    return;
                }
                writeValue(el, f.attr, ch.value);
                applied++;
            });
            return {applied: applied, errors: errors};
        },
        js: async function (c) {
            // indirektes eval: globaler Scope, Ergebnis ist der Wert der letzten Anweisung, Promises werden abgewartet
            let value;
            try {
                value = (0, eval)(c.code);
            } catch (e) {
                // await geht nur in einer async-Funktion: als Ausdruck, sonst als Rumpf (Ergebnis per return)
                if (!(e instanceof SyntaxError) || !/\bawait\b/.test(c.code)) throw e;
                const AsyncFunction = (async function () {}).constructor;
                let fn;
                try { fn = new AsyncFunction('return (' + c.code + '\n);'); } catch (e2) { fn = new AsyncFunction(c.code); }
                value = await fn();
            }
            if (value instanceof Promise) value = await value;
            return {value: serialize(value, 0), type: Object.prototype.toString.call(value).slice(8, -1)};
        },
        click: function (c) {
            const el = document.querySelectorAll(c.selector)[c.index || 0];
            if (!el) throw new Error('Element nicht gefunden');
            el.click();
            return {};
        }
    };

    async function run(c) {
        try {
            const fn = COMMANDS[c.cmd];
            if (!fn) throw new Error('Unbekannter Befehl ' + c.cmd);
            return {success: true, ...await fn(c)};
        } catch (e) {
            return {success: false, message: e instanceof Error ? e.name + ': ' + e.message : String(e)};
        }
    }

    // --- Long-Polling -----------------------------------------------------------------------

    async function loop() {
        let connected = null;
        for (;;) {
            try {
                const res = await request('/api/poll', {client: CLIENT, url: location.href, title: document.title}, 40000);
                if (connected !== true) console.info('[nxrunner] verbunden mit ' + SERVER + ' als Tab ' + CLIENT);
                connected = true;
                for (const c of res.commands || []) {
                    await request('/api/result', {client: CLIENT, id: c.id, result: await run(c)}, 10000);
                }
            } catch (e) {
                if (connected !== false) console.warn('[nxrunner] keine Verbindung zu ' + SERVER + ': ' + (e && e.message || e));
                connected = false;
                await new Promise(function (r) { setTimeout(r, 3000); });
            }
        }
    }

    console.info('[nxrunner] WebsiteController-Script gestartet (' + (typeof GM_xmlhttpRequest === 'undefined' ? 'fetch' : 'GM_xmlhttpRequest') + ')');
    loop();
})();
