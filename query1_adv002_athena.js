import { runAthenaQuery } from './athenaClient.js';

(async () => {
    try {
        const query = `
WITH company_exploded AS (
  SELECT explode(production_companies) AS company, status, vote_average
  FROM movies WHERE production_companies IS NOT NULL
),
company_counts AS (
  SELECT company.name AS company_name, COUNT(*) AS num_movies
  FROM company_exploded GROUP BY company.name HAVING COUNT(*) > 1
)
SELECT ce.company.name AS company, ce.status, STDDEV(ce.vote_average) AS vote_stddev, AVG(ce.vote_average) AS avg_vote
FROM company_exploded ce
JOIN company_counts cc ON ce.company.name = cc.company_name
GROUP BY ce.company.name, ce.status
HAVING STDDEV(ce.vote_average) > 0.5
ORDER BY vote_stddev DESC
LIMIT 10;
        `;
        const { results, execTime } = await runAthenaQuery(query);
        console.log('Resultados:', JSON.stringify(results.Rows, null, 2));
        console.log(`Tiempo total: ${execTime.toFixed(2)}s`);
        fs.appendFileSync('adaptations.log', `Query 1 resultados: ${JSON.stringify(results.Rows.slice(0, 5), null, 2)}\n`);
    } catch (err) {
        console.error('Error:', err);
    }
})();
