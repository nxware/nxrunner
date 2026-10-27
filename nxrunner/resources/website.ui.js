(function () {
    'use strict';

    const TYPE = __TYPE__;
    const STORE = 'nx_website_' + TYPE;
    const $ = function (id) { return document.getElementById(id); };

    // --- Hilfsfunktionen --------------------------------------------------------------------

    async function job(data) {
        const body = new URLSearchParams();
        body.append('type', TYPE);
        for (const k in data) {
            body.append(k, typeof data[k] === 'object' ? JSON.stringify(data[k]) : data[k]);
        }
        const res = await fetch(location.protocol + '//' + location.host + '/', {method: 'POST', body: body});
        return res.json();
    }

    function el(tag, text, attrs) {
        const e = document.createElement(tag);
        if (text !== undefined && text !== null) e.textContent = text;
        for (const k in attrs || {}) e.setAttribute(k, attrs[k]);
        return e;
    }

    function message(target, text, error) {
        target.replaceChildren(el('div', text, {'class': error ? 'alert alert-error' : 'alert'}));
    }

    function table(columns, rows, cell) {
        const t = el('table', null, {'class': 'ws_table'});
        const head = el('tr');
        columns.forEach(function (c) { head.append(el('th', c)); });
        t.append(el('thead'));
        t.tHead.append(head);
        const tbody = el('tbody');
        rows.forEach(function (row, i) {
            const tr = el('tr');
            columns.forEach(function (c, j) { tr.append(cell(row, c, i, j)); });
            tbody.append(tr);
        });
        t.append(tbody);
        return t;
    }

    function client() {
        return $('ws_client').value;
    }

    function save() {
        try {
            localStorage.setItem(STORE, JSON.stringify({selector: $('ws_selector').value, base: $('ws_base').value, fields: fields()}));
        } catch (e) {}
    }

    function load() {
        try { return JSON.parse(localStorage.getItem(STORE)) || {}; } catch (e) { return {}; }
    }

    // --- Tabs -------------------------------------------------------------------------------

    async function reloadClients() {
        const sel = $('ws_client');
        const current = sel.value;
        const res = await job({op: 'clients'});
        sel.replaceChildren();
        (res.value || []).forEach(function (c) {
            const o = el('option', (c.title || '(ohne Titel)') + ' – ' + c.url, {value: c.client});
            if (c.client === current) o.selected = true;
            sel.append(o);
        });
        if (sel.options.length === 0) sel.append(el('option', 'Kein Tab verbunden', {value: ''}));
    }

    // --- CSS-Selektor -----------------------------------------------------------------------

    async function query() {
        save();
        const out = $('ws_query_result');
        const res = await job({op: 'query', selector: $('ws_selector').value, client: client()});
        if (!res.success) return message(out, res.message, true);
        out.replaceChildren(el('div', res.count + ' Treffer' + (res.count > res.items.length ? ', ' + res.items.length + ' angezeigt' : '')));
        out.append(table(['#', 'Tag', 'Attribute', 'Text / Wert', 'Kinder'], res.items, function (item, c) {
            if (c === '#') return el('td', item.i);
            if (c === 'Tag') return el('td', item.tag);
            if (c === 'Kinder') return el('td', item.children);
            if (c === 'Attribute') {
                const td = el('td');
                for (const k in item.attrs) td.append(el('div', k + '="' + item.attrs[k] + '"'));
                return td;
            }
            return el('td', item.value !== null ? item.value : item.text);
        }));
    }

    async function highlight(selector) {
        const res = await job({op: 'highlight', selector: selector, client: client()});
        if (!res.success) message($('ws_query_result'), res.message, true);
    }

    // --- Strukturierte Daten ----------------------------------------------------------------

    const PLACEHOLDER = {name: 'Spaltenname', selector: 'z.B. .price, input[1]', attr: 'z.B. href'};
    let data = null;       // {base, fields, columns, rows}
    let changes = {};      // "row|field" -> value

    function addField(f) {
        f = f || {};
        const tr = el('tr');
        ['name', 'selector', 'attr'].forEach(function (k) {
            const td = el('td');
            td.append(el('input', null, {'data-key': k, value: f[k] || '', placeholder: PLACEHOLDER[k]}));
            tr.append(td);
        });
        const td = el('td');
        const del = el('button', '✕', {type: 'button'});
        del.onclick = function () { tr.remove(); save(); };
        td.append(del);
        tr.append(td);
        $('ws_fields').tBodies[0].append(tr);
    }

    function fields() {
        return Array.from($('ws_fields').tBodies[0].rows).map(function (tr) {
            const f = {};
            tr.querySelectorAll('input').forEach(function (i) { f[i.dataset.key] = i.value.trim(); });
            return f;
        }).filter(function (f) { return f.name; });
    }

    function updateApply() {
        const n = Object.keys(changes).length;
        $('ws_apply').disabled = n === 0;
        $('ws_apply').textContent = 'Änderungen zur Webseite übertragen' + (n ? ' (' + n + ')' : '');
    }

    async function extract() {
        save();
        const out = $('ws_data_result');
        const f = fields();
        const res = await job({op: 'extract', base: $('ws_base').value, fields: f, client: client()});
        changes = {};
        updateApply();
        if (!res.success) { data = null; return message(out, res.message, true); }
        data = {base: $('ws_base').value, fields: f, client: res.client, rows: res.rows};
        out.replaceChildren(el('div', res.count + ' Zeilen von ' + res.url));
        out.append(table(['#'].concat(res.columns), res.rows, function (row, c, i, j) {
            if (j === 0) return el('td', i);
            const td = el('td');
            const original = row[c] === null ? '' : String(row[c]);
            const input = el(original.includes('\n') ? 'textarea' : 'input', null, {});
            input.value = original;
            if (row[c] === null) input.placeholder = '(nicht gefunden)';
            input.oninput = function () {
                const key = i + '|' + c;
                if (input.value === original) delete changes[key]; else changes[key] = {row: i, field: c, value: input.value};
                input.classList.toggle('ws_changed', key in changes);
                updateApply();
            };
            td.append(input);
            return td;
        }));
    }

    async function apply() {
        const out = $('ws_data_result');
        const res = await job({op: 'apply', base: data.base, fields: data.fields, changes: Object.values(changes), client: data.client});
        if (!res.success) return message(out, res.message, true);
        const info = el('div', res.applied + ' Werte übertragen' + (res.errors.length ? ', Fehler: ' + res.errors.join('; ') : ''),
            {'class': res.errors.length ? 'alert alert-error' : 'alert'});
        await extract();
        out.prepend(info);
    }

    // --- Start ------------------------------------------------------------------------------

    const style = el('style', '.ws_table { border-collapse: collapse; margin: 8px 0; } ' +
        '.ws_table td, .ws_table th { border: 1px solid #ccc; padding: 2px 6px; vertical-align: top; text-align: left; } ' +
        '.ws_table input, .ws_table textarea { width: 100%; min-width: 120px; box-sizing: border-box; } ' +
        '.ws_changed { background: #fff3b0; color: #000; } #ws_query_result td div { font-family: monospace; font-size: 0.85em; }');
    document.head.append(style);

    const script = 'http://' + location.hostname + ':__PORT__/website.user.js';
    $('ws_script').href = script;
    $('ws_script').textContent = script;

    const state = load();
    $('ws_selector').value = state.selector || '';
    $('ws_base').value = state.base || '';
    (state.fields && state.fields.length ? state.fields : [{name: 'text'}]).forEach(addField);
    $('ws_fields').addEventListener('change', save);

    $('ws_reload_clients').onclick = reloadClients;
    $('ws_open_tab').onclick = function () {
        if (client()) location.href = __TAB_LINK__ + 'client=' + encodeURIComponent(client());
    };
    $('ws_query').onclick = query;
    $('ws_selector').onkeydown = function (e) { if (e.key === 'Enter') query(); };
    $('ws_highlight').onclick = function () { highlight($('ws_selector').value); };
    $('ws_unhighlight').onclick = function () { highlight(''); };
    $('ws_add_field').onclick = function () { addField(); };
    $('ws_extract').onclick = extract;
    $('ws_apply').onclick = apply;

    reloadClients();
    setInterval(reloadClients, 10000);
})();
