import fs from 'fs-extra';
import path from 'path';
import axios from 'axios';

// config
const ARCHIVE_DIR = 'archive';
const PLAYERS_DIR = 'players';
const CF_WORKER_URL = process.env.CF_WORKER_URL;

async function run() {
    try {
        await fs.ensureDir(PLAYERS_DIR);

        // 1. Load Base Data
        const playerMap = new Map();
        const hasArchive = await fs.pathExists(ARCHIVE_DIR);
        
        const loadDir = hasArchive ? ARCHIVE_DIR : PLAYERS_DIR;
        if (await fs.pathExists(loadDir)) {
            console.log(`Loading base data from ${loadDir}...`);
            const files = await fs.readdir(loadDir);
            for (const file of files) {
                if (!file.endsWith('.json')) continue;
                const data = await fs.readJson(path.join(loadDir, file));
                playerMap.set(data.fc, data);
            }
        }

        // 2. Fetch Cloudflare Data (ROBUST CHECK)
        console.log('Fetching Cloudflare Data...');
        if (!CF_WORKER_URL) throw new Error("CF_WORKER_URL is missing!");

        const historyRes = await axios.get(`${CF_WORKER_URL}/`);
        
        let cfHistoryRows = [];
        
        // --- FIX START: Handle various response types ---
        if (Array.isArray(historyRes.data)) {
            cfHistoryRows = historyRes.data;
        } else if (historyRes.data && Array.isArray(historyRes.data.results)) {
            // D1 sometimes returns object with results array
            cfHistoryRows = historyRes.data.results;
        } else if (typeof historyRes.data === 'string') {
            try {
                const parsed = JSON.parse(historyRes.data);
                cfHistoryRows = Array.isArray(parsed) ? parsed : (parsed.results || []);
            } catch (e) {
                console.error("Could not parse string response from CF:", historyRes.data.substring(0, 100));
            }
        } else {
            console.warn("Unexpected CF response format:", typeof historyRes.data);
        }
        // --- FIX END ---

        // Sort rows
        cfHistoryRows.sort((a, b) => a.timestamp - b.timestamp);
        console.log(`Processing ${cfHistoryRows.length} history points from Cloudflare.`);

        // Fetch discord cache (safe fetch)
        let discordCache = {};
        try {
            const dcRes = await axios.get(`${CF_WORKER_URL}/discord`);
            discordCache = dcRes.data || {};
        } catch (e) {
            console.warn('Discord cache fetch failed (ignoring):', e.message);
        }

        // 3. Process Rows
        cfHistoryRows.forEach(row => {
            const rowDate = new Date(row.timestamp * 1000).toISOString();
            let rowData = row.data;
            if (typeof rowData === 'string') {
                try { rowData = JSON.parse(rowData); } catch(e) { return; }
            }
            
            const playersInRow = Array.isArray(rowData) ? rowData : Object.values(rowData);

            playersInRow.forEach(p => {
                if (!p.fc) return;

                if (!playerMap.has(p.fc)) {
                    playerMap.set(p.fc, {
                        name: p.name || 'Unknown',
                        fc: p.fc,
                        vr_history: [],
                        discord: 'unknown'
                    });
                }

                const entry = playerMap.get(p.fc);
                entry.name = p.name || entry.name;
                
                const currentVR = parseInt(p.ev || p.vr || 0);
                
                // Dedupe timestamp
                const exists = entry.vr_history.some(h => h.date === rowDate);
                if (!exists) {
                     const lastTotal = entry.vr_history.length > 0 ? entry.vr_history[entry.vr_history.length - 1].totalVR : currentVR;
                     entry.vr_history.push({
                         date: rowDate,
                         vrChange: currentVR - lastTotal,
                         totalVR: currentVR
                     });
                }
            });
        });

        // 4. Calculate Stats
        console.log('Finalizing stats...');
        const now = new Date();
        const oneDay = 24 * 60 * 60 * 1000;
        const sevenDays = 7 * oneDay;
        const thirtyDays = 30 * oneDay;

        for (const [fc, data] of playerMap) {
            data.vr_history.sort((a, b) => new Date(a.date) - new Date(b.date));
            data.vr_history = data.vr_history.filter(h => (now - new Date(h.date)) <= thirtyDays);

            if (data.vr_history.length === 0) continue;

            const currentTotal = data.vr_history[data.vr_history.length - 1].totalVR;

            const getVRAt = (msAgo) => {
                const targetTime = now.getTime() - msAgo;
                const entry = data.vr_history.find(h => new Date(h.date).getTime() >= targetTime);
                return entry ? entry.totalVR : (data.vr_history[0].totalVR);
            };

            data.vrStats = {
                last24Hours: currentTotal - getVRAt(oneDay),
                lastWeek: currentTotal - getVRAt(sevenDays),
                lastMonth: currentTotal - getVRAt(thirtyDays)
            };

            // Discord Logic
            const cacheProfile = discordCache[fc];
            if (!data.discord) data.discord = 'unknown';
            if (cacheProfile) {
                if (cacheProfile === 'not_linked') {
                     // Keep existing valid profile if we have one, otherwise set not_linked
                    if (data.discord === 'unknown' || data.discord === 'not_linked') {
                        data.discord = 'not_linked';
                    }
                } else {
                    data.discord = cacheProfile;
                }
            }

            // Save
            const safeFC = fc.replace(/[^a-zA-Z0-9-]/g, '_');
            await fs.writeJson(path.join(PLAYERS_DIR, `${safeFC}.json`), data, { spaces: 2 });
        }

        // 5. Cleanup Archive
        if (hasArchive) {
            await fs.remove(ARCHIVE_DIR);
        }

        console.log('Done.');

    } catch (e) {
        console.error("FATAL ERROR:", e);
        process.exit(1);
    }
}

run();