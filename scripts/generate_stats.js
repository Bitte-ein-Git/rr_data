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

        // 1. Load Base Data (Archive > Existing Players)
        const playerMap = new Map();
        const hasArchive = await fs.pathExists(ARCHIVE_DIR);
        
        // Prioritize Archive for backfill runs
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

        // 2. Fetch Cloudflare Data
        console.log('Fetching Cloudflare Data...');
        if (!CF_WORKER_URL) throw new Error("CF_WORKER_URL env var missing");

        const historyRes = await axios.get(`${CF_WORKER_URL}/`);
        let cfHistoryRows = [];

        // Robust parsing of CF response
        if (Array.isArray(historyRes.data)) {
            cfHistoryRows = historyRes.data;
        } else if (historyRes.data && Array.isArray(historyRes.data.results)) {
            cfHistoryRows = historyRes.data.results;
        } else if (typeof historyRes.data === 'string') {
            try {
                const parsed = JSON.parse(historyRes.data);
                cfHistoryRows = Array.isArray(parsed) ? parsed : (parsed.results || []);
            } catch (e) {
                console.warn("CF data parse error, skipping update.");
            }
        }

        cfHistoryRows.sort((a, b) => a.timestamp - b.timestamp);
        console.log(`Processing ${cfHistoryRows.length} live data points...`);

        // Fetch Discord Cache
        let discordCache = {};
        try {
            const dcRes = await axios.get(`${CF_WORKER_URL}/discord`);
            discordCache = dcRes.data || {};
        } catch (e) { /* ignore */ }

        // 3. Process Live Data
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
                
                // Avoid duplicate timestamps
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

        // 4. Finalize & Stats
        console.log('Calculating stats...');
        const now = new Date();
        const oneDay = 24 * 60 * 60 * 1000;
        const sevenDays = 7 * oneDay;
        const thirtyDays = 30 * oneDay;

        for (const [fc, data] of playerMap) {
            // Sort history
            data.vr_history.sort((a, b) => new Date(a.date) - new Date(b.date));
            // Prune > 30d
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

            // Discord Profile Sync
            const cacheProfile = discordCache[fc];
            if (!data.discord) data.discord = 'unknown';
            
            if (cacheProfile) {
                if (cacheProfile === 'not_linked') {
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

        // 5. Cleanup
        if (hasArchive) {
            console.log('Removing archive...');
            await fs.remove(ARCHIVE_DIR);
        }

        console.log('Update Complete.');

    } catch (e) {
        console.error(e);
        process.exit(1);
    }
}

run();