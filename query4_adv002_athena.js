import { runAthenaQuery } from './athenaClient.js';
import fs from 'fs';

(async () => {
    try {
        const query = `
WITH country_exploded AS (
  SELECT explode(production_countries) AS country, status, CAST(revenue AS DOUBLE) / budget AS roi
  FROM movies WHERE budget > 0 AND revenue > 0
)
SELECT country.name AS country, status, VARIANCE(roi) AS roi_variance, AVG(roi) AS avg_roi,
  ROW_NUMBER() OVER (PARTITION BY status ORDER BY VARIANCE(roi) DESC) AS rank_variance
FROM country_exploded
GROUP BY country.name, status
HAVING AVG(roi) > 1
ORDER BY status, rank_variance
LIMIT 10;
        `;
        const { results, execTime } = await runAthenaQuery(query);
        console.log('Resultados:', JSON.stringify(results.Rows, null, 2));
        console.log(`Tiempo total: ${execTime.toFixed(2)}s`);
        fs.appendFileSync('adaptations.log', `Query 4 resultados: ${JSON.stringify(results.Rows.slice(0, 5), null, 2)}\n`);
    } catch (err) {
        console.error('Error:', err);
    }
})();
