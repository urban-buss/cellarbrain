/* drink-tonight.js — local-first management of the dashboard's
 * "drink tonight" shortlist with server-side mirror via /drink-tonight.
 *
 *  storage key: cb.drink_tonight
 *  shape: [{wine_id: int, added_at: ISO, note?: string}]
 */
(function () {
    "use strict";
    const STORAGE_KEY = "cb.drink_tonight";

    function readLocal() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            if (!raw) return [];
            const v = JSON.parse(raw);
            return Array.isArray(v) ? v : [];
        } catch (e) {
            return [];
        }
    }

    function writeLocal(items) {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(items));
        } catch (e) {
            /* ignore quota errors */
        }
    }

    function syncToServer(items) {
        return fetch("/drink-tonight", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ items: items }),
        }).then((r) => (r.ok ? r.json() : null));
    }

    async function bootstrap() {
        let local = readLocal();
        if (local.length === 0) {
            // localStorage empty (new device or cleared) — pull from server
            try {
                const r = await fetch("/drink-tonight.json");
                if (r.ok) {
                    const data = await r.json();
                    if (data.items && data.items.length) {
                        writeLocal(data.items);
                    }
                }
            } catch (e) { /* offline — ignore */ }
        } else {
            // We have local state — push it to keep sidecar in sync
            syncToServer(local).catch(() => {});
        }
    }

    function addLocal(wineId, note) {
        const items = readLocal();
        if (items.some((it) => Number(it.wine_id) === Number(wineId))) return items;
        items.push({
            wine_id: Number(wineId),
            added_at: new Date().toISOString(),
            note: note || null,
        });
        writeLocal(items);
        return items;
    }

    function removeLocal(wineId) {
        const items = readLocal().filter(
            (it) => Number(it.wine_id) !== Number(wineId)
        );
        writeLocal(items);
        return items;
    }

    // Mirror htmx button outcomes into localStorage
    document.body.addEventListener("htmx:afterRequest", function (e) {
        const tgt = e.target;
        if (!tgt) return;
        if (tgt.dataset && tgt.dataset.drinkTonightAdd) {
            addLocal(tgt.dataset.drinkTonightAdd);
        } else if (tgt.dataset && tgt.dataset.drinkTonightRemove) {
            removeLocal(tgt.dataset.drinkTonightRemove);
        }
    });

    document.addEventListener("DOMContentLoaded", bootstrap);
})();
