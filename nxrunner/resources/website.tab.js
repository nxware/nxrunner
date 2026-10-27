(function () {
    'use strict';

    const TYPE = __TYPE__;
    const CLIENT = __CLIENT__;
    const STORE = 'nx_website_js_' + TYPE;
    const $ = function (id) { return document.getElementById(id); };

    async function job(data) {
        const body = new URLSearchParams();
        body.append('type', TYPE);
        for (const k in data) body.append(k, data[k]);
        const res = await fetch(location.protocol + '//' + location.host + '/', {method: 'POST', body: body});
        return res.json();
    }

    function el(tag, text, attrs) {
        const e = document.createElement(tag);
        if (text !== undefined && text !== null) e.textContent = text;
        for (const k in attrs || {}) e.setAttribute(k, attrs[k]);
        return e;
    }

    async function run() {
        const code = $('ws_code').value;
        try { localStorage.setItem(STORE, code); } catch (e) {}
        $('ws_run').disabled = true;
        const started = Date.now();
        let res;
        try {
            res = await job({op: 'eval_js', code: code, client: CLIENT});
        } catch (e) {
            res = {success: false, message: String(e)};
        }
        $('ws_run').disabled = false;
        // neuestes Ergebnis oben, die vorherigen bleiben darunter stehen
        const entry = el('div', null, {'class': 'ws_js_entry'});
        entry.append(el('pre', code, {'class': 'ws_js_code'}));
        const info = (Date.now() - started) + ' ms' + (res.type ? ', ' + res.type : '');
        if (res.success) {
            entry.append(el('small', info));
            entry.append(el('pre', typeof res.value === 'string' ? res.value : JSON.stringify(res.value, null, 2)));
        } else {
            entry.append(el('div', res.message, {'class': 'alert alert-error'}));
        }
        $('ws_js_result').prepend(entry);
    }

    document.head.append(el('style', '.ws_js_entry { border-top: 1px solid #ccc; margin-top: 8px; padding-top: 4px; } ' +
        '.ws_js_code { opacity: 0.6; margin: 0; } .ws_js_entry pre { white-space: pre-wrap; word-break: break-all; }'));

    try { $('ws_code').value = localStorage.getItem(STORE) || ''; } catch (e) {}
    $('ws_run').onclick = run;
    $('ws_code').onkeydown = function (e) {
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); run(); }
    };
})();
