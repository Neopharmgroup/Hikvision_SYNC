const axios = require('axios');
const xml2js = require('xml2js');
const sql = require('mssql');
const schedule = require('node-schedule');
const moment = require('moment-timezone');
require('dotenv').config();

// הגדרות חיבור ל-SQL
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

// משתנה גלובלי לחיבור
let sqlPool = null;

// פונקציה להתחברות ל-SQL
async function connectToSQL() {
    try {
        sqlPool = await sql.connect(sqlConfig);
        console.log('✅ התחברנו ל-SQL Server בהצלחה!');
    } catch (error) {
        console.error('❌ שגיאה בחיבור ל-SQL:', error.message);
        process.exit(1);
    }
}

// פונקציה לקבלת זמן הסנכרון האחרון
async function getLastSyncTime() {
    try {
        const result = await sqlPool.request()
            .query('SELECT MAX(CaptureTime) as LastTime FROM VehicleDetection');

        if (result.recordset[0].LastTime) {
            return result.recordset[0].LastTime;
        } else {
            // אם אין נתונים, התחל מהיום
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            return today;
        }
    } catch (error) {
        console.error('שגיאה בקריאת זמן אחרון:', error.message);
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        return today;
    }
}

// פונקציה לקריאת נתונים מהמצלמה
async function fetchVehiclesFromCamera(fromTime) {
    const vehicles = [];
    try {
        // ייבוא דינמי של DigestFetch
        const { default: DigestFetch } = await import('digest-fetch');

        // המר לזמן ישראל
        const israelTime = moment(fromTime).tz('Asia/Jerusalem');
        const picTime = israelTime.format('YYYY-MM-DDTHH:mm:ss');

        const requestBody = `<?xml version="1.0" encoding="UTF-8"?>
<AfterTime version="2.0" xmlns="http://www.hikvision.com/ver20/XMLSchema">
    <picTime>${picTime}</picTime>
</AfterTime>`;

        console.log(`🔍 מחפש רכבים מ: ${picTime} (זמן ישראל)`);

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

        console.log(`✅ נמצאו ${vehicles.length} רכבים חדשים`);
    } catch (error) {
        console.error('❌ שגיאה בקריאה מהמצלמה:', error);
    }

    return vehicles;
}

// פונקציה להכנסת נתונים ל-SQL
async function insertVehiclesToSQL(vehicles) {
    if (!vehicles || vehicles.length === 0) {
        return 0;
    }

    let inserted = 0;

    for (const vehicle of vehicles) {
        try {
            // בדיקה אם הרשומה כבר קיימת
            const checkResult = await sqlPool.request()
                .input('picName', sql.NVarChar(100), vehicle.picName)
                .query('SELECT 1 FROM VehicleDetection WHERE PicName = @picName');

            if (checkResult.recordset.length === 0) {
                // הכנסת רשומה חדשה
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
            console.error(`שגיאה בהכנסת רכב ${vehicle.plateNumber}:`, error.message);
        }
    }

    console.log(`💾 נשמרו ${inserted} רשומות חדשות ב-SQL`);
    return inserted;
}

// פונקציה ראשית לסנכרון
async function syncVehicles() {
    console.log('\n' + '='.repeat(50));
    console.log(`🚗 מתחיל סנכרון - ${new Date().toLocaleString('he-IL')}`);
    console.log('='.repeat(50));

    try {
        // קבל זמן סנכרון אחרון
        const lastSync = await getLastSyncTime();
        console.log(`⏱️  זמן סנכרון אחרון: ${lastSync.toLocaleString('he-IL')}`);

        // קרא נתונים חדשים מהמצלמה
        const vehicles = await fetchVehiclesFromCamera(lastSync);

        // הכנס לטבלה
        if (vehicles.length > 0) {
            await insertVehiclesToSQL(vehicles);
        } else {
            console.log('📭 אין רכבים חדשים');
        }

        console.log('✅ הסנכרון הושלם בהצלחה!\n');
    } catch (error) {
        console.error('❌ שגיאה בסנכרון:', error.message);
    }
}

// פונקציה ראשית
async function main() {
    console.log('🚀 מערכת סנכרון רכבים Hikvision');
    console.log('================================\n');

    // התחבר ל-SQL
    await connectToSQL();

    // סנכרון ראשוני
    await syncVehicles();

    // תזמון כל X דקות
    const intervalMinutes = parseInt(process.env.SYNC_INTERVAL_MINUTES) || 2;
    console.log(`⏰ מתזמן סנכרון כל ${intervalMinutes} דקות`);

    // הגדרת תזמון
    const job = schedule.scheduleJob(`*/${intervalMinutes} * * * *`, async () => {
        await syncVehicles();
    });

    console.log('📡 המערכת פועלת. לחץ Ctrl+C לעצירה\n');

    // טיפול ביציאה
    process.on('SIGINT', async () => {
        console.log('\n👋 עוצר את המערכת...');
        job.cancel();
        await sqlPool.close();
        process.exit(0);
    });
}

// הפעלה
main().catch(error => {
    console.error('❌ שגיאה קריטית:', error);
    process.exit(1);
});