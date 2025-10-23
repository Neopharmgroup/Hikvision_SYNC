const axios = require('axios');
const xml2js = require('xml2js');
const sql = require('mssql');
const schedule = require('node-schedule');
const moment = require('moment-timezone');
require('dotenv').config();

// SQL connection settings
const sqlConfig = {
    user: process.env.SQL_USERNAME,
    password: process.env.SQL_PASSWORD,
    database: process.env.SQL_DATABASE,
    server: process.env.SQL_SERVER,
    port: parseInt(process.env.SQL_PORT) || 1433,
    options: {
        encrypt: true,
        trustServerCertificate: true,
        useUTC: false
    }
};

// Global variable for connection
let sqlPool = null;

// Function to connect to SQL
async function connectToSQL() {
    try {
        sqlPool = await sql.connect(sqlConfig);
        console.log('✅ Successfully connected to SQL Server!');
    } catch (error) {
        console.error('❌ Error connecting to SQL:', error.message);
        process.exit(1);
    }
}

// Function to get last sync time
async function getLastSyncTime(tableName) {
    try {
        const result = await sqlPool.request()
            .query(`SELECT MAX(CaptureTime) as LastTime FROM ${tableName}`);

        if (result.recordset[0].LastTime) {
            return result.recordset[0].LastTime;
        } else {
            // If no data exists, start from today
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            return today;
        }
    } catch (error) {
        console.error(`Error reading last sync time from ${tableName}:`, error.message);
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        return today;
    }
}

// Function to read data from camera
async function fetchVehiclesFromCamera(fromTime, cameraConfig) {
    const vehicles = [];
    try {
        // Dynamic import of DigestFetch
        const { default: DigestFetch } = await import('digest-fetch');

        // Convert to Israel time
        const israelTime = moment(fromTime).tz('Asia/Jerusalem');
        const picTime = israelTime.format('YYYY-MM-DDTHH:mm:ss');

        const requestBody = `<?xml version="1.0" encoding="UTF-8"?>
<AfterTime version="2.0" xmlns="http://www.hikvision.com/ver20/XMLSchema">
    <picTime>${picTime}</picTime>
</AfterTime>`;

        console.log(`🔍 [${cameraConfig.name}] Searching vehicles from: ${picTime} (Israel time)`);

        // Digest Auth
        const client = new DigestFetch(
            cameraConfig.username,
            cameraConfig.password
        );

        const res = await client.fetch(
            `${cameraConfig.url}/ISAPI/Traffic/channels/1/vehicleDetect/plates`,
            {
                method: 'POST',
                body: requestBody,
                headers: { 'Content-Type': 'application/xml' },
                timeout: 30000
            }
        );

        if (!res.ok) {
            throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        }

        const xml = await res.text();

        const parser = new xml2js.Parser();
        const result = await parser.parseStringPromise(xml);

        if (result.Plates && result.Plates.Plate) {
            const plates = Array.isArray(result.Plates.Plate)
                ? result.Plates.Plate
                : [result.Plates.Plate];

            for (const plate of plates) {
                const captureTimeStr = plate.captureTime[0];
                const year = captureTimeStr.substring(0, 4);
                const month = captureTimeStr.substring(4, 6);
                const day = captureTimeStr.substring(6, 8);
                const hour = captureTimeStr.substring(9, 11);
                const minute = captureTimeStr.substring(11, 13);
                const second = captureTimeStr.substring(13, 15);

                vehicles.push({
                    captureTime: moment.tz(`${year}-${month}-${day}T${hour}:${minute}:${second}`, 'Asia/Jerusalem').toDate(),
                    plateNumber: plate.plateNumber[0],
                    picName: plate.picName[0],
                    country: plate.country[0],
                    direction: plate.direction[0]
                });
            }
        }

        console.log(`✅ [${cameraConfig.name}] Found ${vehicles.length} new vehicles`);
    } catch (error) {
        console.error(`❌ [${cameraConfig.name}] Error reading from camera:`, error);
    }

    return vehicles;
}

// Function to insert data to SQL
async function insertVehiclesToSQL(vehicles, tableName, cameraName) {
    if (!vehicles || vehicles.length === 0) {
        return 0;
    }

    let inserted = 0;

    for (const vehicle of vehicles) {
        try {
            // Check if record already exists
            const checkResult = await sqlPool.request()
                .input('picName', sql.NVarChar(100), vehicle.picName)
                .query(`SELECT 1 FROM ${tableName} WHERE PicName = @picName`);

            if (checkResult.recordset.length === 0) {
                // Insert new record
                await sqlPool.request()
                    .input('captureTime', sql.DateTime2, vehicle.captureTime)
                    .input('plateNumber', sql.NVarChar(50), vehicle.plateNumber)
                    .input('picName', sql.NVarChar(100), vehicle.picName)
                    .input('country', sql.NVarChar(10), vehicle.country)
                    .input('direction', sql.NVarChar(20), vehicle.direction)
                    .input('insertTime', sql.DateTime2, new Date()) 

                    .query(`
                        INSERT INTO ${tableName} 
                        (CaptureTime, PlateNumber, PicName, Country, Direction, InsertTime )
                        VALUES (@captureTime, @plateNumber, @picName, @country, @direction, @insertTime)
                    `);

                inserted++;
                console.log(`   ➕ [${cameraName}] ${vehicle.plateNumber} - ${vehicle.direction}`);
            }
        } catch (error) {
            console.error(`[${cameraName}] Error inserting vehicle ${vehicle.plateNumber}:`, error.message);
        }
    }

    console.log(`💾 [${cameraName}] Saved ${inserted} new records to SQL`);
    return inserted;
}

// Main sync function for a specific camera
async function syncVehicles(cameraConfig) {
    console.log('\n' + '='.repeat(50));
    console.log(`🚗 [${cameraConfig.name}] Starting sync - ${new Date().toLocaleString('he-IL')}`);
    console.log('='.repeat(50));

    try {
        // Get last sync time
        const lastSync = await getLastSyncTime(cameraConfig.tableName);
        console.log(`⏱️  [${cameraConfig.name}] Last sync time: ${lastSync.toLocaleString('he-IL')}`);

        // Read new data from camera
        const vehicles = await fetchVehiclesFromCamera(lastSync, cameraConfig);

        // Insert to table
        if (vehicles.length > 0) {
            await insertVehiclesToSQL(vehicles, cameraConfig.tableName, cameraConfig.name);
        } else {
            console.log(`📭 [${cameraConfig.name}] No new vehicles found`);
        }

        console.log(`✅ [${cameraConfig.name}] Sync completed successfully!\n`);
    } catch (error) {
        console.error(`❌ [${cameraConfig.name}] Sync error:`, error.message);
    }
}

// Sync all cameras
async function syncAllCameras(cameras) {
    for (const camera of cameras) {
        await syncVehicles(camera);
    }
}

// Main function
async function main() {
    console.log('🚀 Hikvision Vehicle Sync System');
    console.log('================================\n');

    // Define cameras configuration
    const cameras = [];
    
    // Camera 1
    if (process.env.CAMERA1_URL) {
        cameras.push({
            name: 'Camera1',
            url: process.env.CAMERA1_URL,
            username: process.env.CAMERA1_USERNAME,
            password: process.env.CAMERA1_PASSWORD,
            tableName: 'VehicleDetection'
        });
    }
    
    // Camera 2
    if (process.env.CAMERA2_URL) {
        cameras.push({
            name: 'Camera2',
            url: process.env.CAMERA2_URL,
            username: process.env.CAMERA2_USERNAME,
            password: process.env.CAMERA2_PASSWORD,
            tableName: 'VehicleDetection2'
        });
    }

    if (cameras.length === 0) {
        console.error('❌ No cameras configured! Please set CAMERA1_URL or CAMERA2_URL in .env file');
        process.exit(1);
    }

    console.log(`📷 Found ${cameras.length} camera(s) configured:`);
    cameras.forEach(cam => console.log(`   - ${cam.name}: ${cam.url} -> ${cam.tableName}`));
    console.log();

    // Connect to SQL
    await connectToSQL();

    // Initial sync for all cameras
    await syncAllCameras(cameras);

    // Schedule every X minutes
    const intervalMinutes = parseInt(process.env.SYNC_INTERVAL_MINUTES) || 2;
    console.log(`⏰ Scheduling sync every ${intervalMinutes} minutes for all cameras`);

    // Set schedule
    const job = schedule.scheduleJob(`*/${intervalMinutes} * * * *`, async () => {
        await syncAllCameras(cameras);
    });

    console.log('📡 System is running. Press Ctrl+C to stop\n');

    // Handle exit
    process.on('SIGINT', async () => {
        console.log('\n👋 Stopping system...');
        job.cancel();
        await sqlPool.close();
        process.exit(0);
    });
}

// Run
main().catch(error => {
    console.error('❌ Critical error:', error);
    process.exit(1);
});