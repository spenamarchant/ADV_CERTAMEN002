import { runAthenaQuery } from './athenaClient.js';
import fs from 'fs';

(async () => {
    try {
        const query = `
WITH prod_exploded AS (
  SELECT FLOOR(YEAR(release_date)/10)*10 AS decade, SIZE(production_companies) AS num_companies, SIZE(production_countries) AS num_countries, vote_count
  FROM movies WHERE production_companies IS NOT NULL AND production_countries IS NOT NULL
)
SELECT decade, AVG(CAST(num_companies AS DOUBLE)) AS avg_num_companies, AVG(CAST(num_countries AS DOUBLE)) AS avg_num_countries,
  CORR(vote_count, num_companies + num_countries) AS corr_complexity_votes, SUM(vote_count) AS total_votes
FROM prod_exploded
GROUP BY decade
ORDER BY decade DESC
LIMIT 10;
        `;
        const { results, execTime } = await runAthenaQuery(query);
        console.log('Resultados:', JSON.stringify(results.Rows, null, 2));
        console.log(`Tiempo total: ${execTime.toFixed(2)}s`);
        fs.appendFileSync('adaptations.log', `Query 5 resultados: ${JSON.stringify(results.Rows.slice(0, 5), null, 2)}\n`);
    } catch (err) {
        console.error('Error:', err);
    }
})();
