const express = require('express')
const bodyParser = require("body-parser");
const app = express()
const path = require('path')
const edge = require('edge.js')
const { Pool, Client } = require('pg')
const client = new Client()
const port = 3000

app.use('/public', express.static(__dirname + '/public'));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());


mvpnonprod = 'se3db-rdscluster-1w62lo6ujynaw.cluster-c8fexpizwlcn.us-east-1.rds.amazonaws.com'




const pool = new Pool({
	user: 'sa',
  host: mvpnonprod,
  database: 'se3',
  password: 'masterkey',
}
)	



edge.registerViews(path.join(__dirname, './view'))


app.get('/', (req, res) => {

	console.log( 'GET props')
	res.send(
		edge.render('t', {})
		)	
	}
	
)


app.post('/getComps', (req, res) => {

	console.log( 'rollups')
	console.log(JSON.stringify(req.body ) ) 	
	let placeId = req.body.placeId
	let sql = `
	select
t1.*,

(
	coalesce(dist_norm,0 ) * 1  + 
	coalesce(year_built_dist_norm,0)  * 1   + 
	coalesce(sale_price_per_unit_dist_norm ,0 ) * sale_date_dist_norm
	 
) / ( 
	coalesce(dist_norm - dist_norm + 1, 0 )  + 
	coalesce( year_built_dist_norm - year_built_dist_norm + 1,0 )  + 
	coalesce(sale_price_per_unit_dist_norm  - sale_price_per_unit_dist_norm + 1,0 )  * sale_date_dist_norm
	) as score
from
(

select
	t0.id,
	1 - (dist - dist_min) / ( dist_max - dist_min ) dist_norm ,
	1 -  ((year_built_dist + 0.0) - year_built_dist_min) / ( year_built_dist_max - year_built_dist_min ) year_built_dist_norm ,
	1 - ( sale_price_per_unit_dist  - sale_price_per_unit_dist_min ) / ( sale_price_per_unit_dist_max - sale_price_per_unit_dist_min ) sale_price_per_unit_dist_norm ,
	1 - ( (sale_date_dist + 0.0)  - sale_date_dist_min ) / ( sale_date_dist_max - sale_date_dist_min ) sale_date_dist_norm ,
	sale_date,
	year_built,
	dist,
	sale_date


from
	(
select 
p."placeId" id,
ST_Distance( subj.point::geometry, p.point::geometry ) dist,
min( ST_Distance( subj.point, p.point ) ) over (  ) dist_min,
max( ST_Distance(  subj.point, p.point ) ) over ( ) dist_max,
p."yearBuilt" as year_built,
abs(subj."yearBuilt" - p."yearBuilt") as year_built_dist,
min( abs(subj."yearBuilt" - p."yearBuilt") ) over () as year_built_dist_min,
max( abs(subj."yearBuilt" - p."yearBuilt") ) over () as year_built_dist_max,
abs(subj."salePricePerUnit" - p."salePricePerUnit")  as sale_price_per_unit_dist,
min( abs(subj."salePricePerUnit" - p."salePricePerUnit") ) over() as sale_price_per_unit_dist_min,
max( abs(subj."salePricePerUnit" - p."salePricePerUnit") ) over() as sale_price_per_unit_dist_max,
abs( subj."saleDateJulian" - p."saleDateJulian" ) as sale_date_dist,
min( abs(subj."saleDateJulian" - p."saleDateJulian") ) over() as sale_date_dist_min,
max( abs(subj."saleDateJulian" - p."saleDateJulian") ) over( ) as sale_date_dist_max,
subj."saleDate" as subj_sale_date,
p."saleDate" as sale_date
from
place p
cross join ( 
	select 
		*
	from
		place 
	where 
	"placeId" = '${placeId}'
		) subj
where
	ST_DWithin(p.point::geography,ST_SetSRID(ST_MakePoint(ST_X(subj.point),ST_Y(subj.point)),4326)::geography,5 * 1609.34)

) t0


) t1

order by score desc
limit 100


	`
	console.log( sql )
	pool.query(sql, [],  (err, data) => {

	console.log( data['rows'] )
	
	res.send(
		edge.render('comps', {data:data['rows']})
		)	
	
	})
}
)



app.listen(port, () => console.log(`Example app listening on port ${port}!`))