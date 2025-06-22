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
async function getLastSyncTime() {
    try {
        const result = await sqlPool.request()
            .query('SELECT MAX(CaptureTime) as LastTime FROM VehicleDetection');

        if (result.recordset[0].LastTime) {
            return result.recordset[0].LastTime;
        } else {
            // If no data exists, start from today
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            return today;
        }
    } catch (error) {
        console.error('Error reading last sync time:', error.message);
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        return today;
    }
}

// Function to read data from camera
async function fetchVehiclesFromCamera(fromTime) {
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

        console.log(`🔍 Searching vehicles from: ${picTime} (Israel time)`);

        // Digest Auth
        const client = new DigestFetch(
            process.env.CAMERA_USERNAME,
            process.env.CAMERA_PASSWORD
        );

        const res = await client.fetch(
            `${process.env.CAMERA_URL}/ISAPI/Traffic/channels/1/vehicleDetect/plates`,
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

        console.log(`✅ Found ${vehicles.length} new vehicles`);
    } catch (error) {
        console.error('❌ Error reading from camera:', error);
    }

    return vehicles;
}

// Function to insert data to SQL
async function insertVehiclesToSQL(vehicles) {
    if (!vehicles || vehicles.length === 0) {
        return 0;
    }

    let inserted = 0;

    for (const vehicle of vehicles) {
        try {
            // Check if record already exists
            const checkResult = await sqlPool.request()
                .input('picName', sql.NVarChar(100), vehicle.picName)
                .query('SELECT 1 FROM VehicleDetection WHERE PicName = @picName');

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
                        INSERT INTO VehicleDetection 
                        (CaptureTime, PlateNumber, PicName, Country, Direction, InsertTime )
                        VALUES (@captureTime, @plateNumber, @picName, @country, @direction, @insertTime)
                    `);

                inserted++;
                console.log(`   ➕ ${vehicle.plateNumber} - ${vehicle.direction}`);
            }
        } catch (error) {
            console.error(`Error inserting vehicle ${vehicle.plateNumber}:`, error.message);
        }
    }

    console.log(`💾 Saved ${inserted} new records to SQL`);
    return inserted;
}

// Main sync function
async function syncVehicles() {
    console.log('\n' + '='.repeat(50));
    console.log(`🚗 Starting sync - ${new Date().toLocaleString('he-IL')}`);
    console.log('='.repeat(50));

    try {
        // Get last sync time
        const lastSync = await getLastSyncTime();
        console.log(`⏱️  Last sync time: ${lastSync.toLocaleString('he-IL')}`);

        // Read new data from camera
        const vehicles = await fetchVehiclesFromCamera(lastSync);

        // Insert to table
        if (vehicles.length > 0) {
            await insertVehiclesToSQL(vehicles);
        } else {
            console.log('📭 No new vehicles found');
        }

        console.log('✅ Sync completed successfully!\n');
    } catch (error) {
        console.error('❌ Sync error:', error.message);
    }
}

// Main function
async function main() {
    console.log('🚀 Hikvision Vehicle Sync System');
    console.log('================================\n');

    // Connect to SQL
    await connectToSQL();

    // Initial sync
    await syncVehicles();

    // Schedule every X minutes
    const intervalMinutes = parseInt(process.env.SYNC_INTERVAL_MINUTES) || 2;
    console.log(`⏰ Scheduling sync every ${intervalMinutes} minutes`);

    // Set schedule
    const job = schedule.scheduleJob(`*/${intervalMinutes} * * * *`, async () => {
        await syncVehicles();
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