import { runAthenaQuery } from './athenaClient.js';
import fs from 'fs';

(async () => {
    try {
        const query = `
WITH lang_exploded AS (
  SELECT original_language, FLOOR(YEAR(release_date)/10)*10 AS decade, revenue, SIZE(spoken_languages) AS num_languages
  FROM movies WHERE spoken_languages IS NOT NULL AND revenue > 0
)
SELECT original_language, decade, AVG(revenue) AS avg_revenue, AVG(CAST(num_languages AS DOUBLE)) AS avg_num_languages, CORR(revenue, num_languages) AS corr_revenue_languages
FROM lang_exploded
GROUP BY original_language, decade
HAVING COUNT(*) > 5
ORDER BY decade DESC, avg_revenue DESC
LIMIT 10;
        `;
        const { results, execTime } = await runAthenaQuery(query);
        console.log('Resultados:', JSON.stringify(results.Rows, null, 2));
        console.log(`Tiempo total: ${execTime.toFixed(2)}s`);
        fs.appendFileSync('adaptations.log', `Query 2 resultados: ${JSON.stringify(results.Rows.slice(0, 5), null, 2)}\n`);
    } catch (err) {
        console.error('Error:', err);
    }
})();
