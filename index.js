const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const axios = require('axios');
const massive = require('massive');
const monitor = require('pg-monitor');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        // Busca os dados fazendo uma requisição GET utilizando o Axios
        const { data: { data, source }} = await axios.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population');

        // Filtra os dados de acordo com os anos solicitados
        const filteredData = data.filter((item) => item.Year >= 2018 && item.Year <= 2020);
        // Realiza o cálculo da soma do campo Population em memória
        const resultUsingArrayFunctions = filteredData.reduce((accumulator, currentValue) => {
            return accumulator = accumulator + currentValue.Population;
        }, 0);

        // Limpa a tabela api_data
        await db[DATABASE_SCHEMA].api_data.destroy({});

        // Insere os dados retornados pela requisição no banco de dados
        await db[DATABASE_SCHEMA].api_data.insert({
            api_name: source[0].name,
            doc_id: source[0].annotations.table_id,
            doc_name: source[0].annotations.dataset_name,
            doc_record: JSON.stringify(data),
        })

        // Retorna a soma da população dos anos solicitados
        const [resultUsingSelect] = await db.query(`
            WITH records AS (
                SELECT jsonb_array_elements(doc_record) AS record
                FROM ${DATABASE_SCHEMA}.api_data
            )
            SELECT SUM((record->>'Population')::integer) AS total_population
            FROM records
            WHERE record->>'Year' IN ('2018', '2019', '2020');
        `);
        
        // Retorna a soma da população dos anos solicitados a partir da view
        const [resultUsingView] = await db[DATABASE_SCHEMA].vw_population.find();

        console.log('Resultado do cálculo em memória >>>', resultUsingArrayFunctions);
        console.log('Resultado do cálculo utilizando select >>>', resultUsingSelect.total_population);
        console.log('Resultado do cálculo utilizando a view >>>', resultUsingView.total_population);
    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();