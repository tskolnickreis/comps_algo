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
	let radius = req.body.radius
	let distCoef = req.body.distCoef
	let salePriceCoef = req.body.salePriceCoef
	let yearBuiltCoef = req.body.yearBuiltCoef
	let sizeCoef = req.body.sizeCoef
	let rentCoef = req.body.rentCoef

	let sql = `

	select
	null as id,
	"streetAddress" as street_address,
	null dist_norm,
	null year_buit_dist_norm,
	null sale_price_per_unit_dist_norm,
	null sale_date_dist_norm,
	null size_unit_dist_norm,
	null size_sqft_dist_norm,
	null rent_per_unit_dist_norm,

	null sale_price_per_unit_dist_norm,
	"salePrice" as sale_price,

	TO_CHAR("saleDate", 'mm/dd/yyyy') as sale_date,
	"yearBuilt" as year_built,
	0 as dist,
	"sizeUnit" as size_unit,
	"salePricePerUnit" as sale_price_per_unit,
	"rentPerUnit" as rent_per_unit,
	100 as score

	from
	place
	where
	"placeId" = '${placeId}'

	union all
	
		select
	t1.*,
	
	(
		coalesce(dist_norm,0 ) * ${distCoef} + 
		coalesce(year_built_dist_norm,0)  * ${yearBuiltCoef}   + 
		coalesce(sale_price_dist_norm ,0 ) * coalesce(sale_date_dist_norm,0)  *  ${salePriceCoef} +
		coalesce(size_unit_dist_norm,0) * ${sizeCoef} +
		coalesce(size_sqft_dist_norm,0) * ${sizeCoef} +
		coalesce(rent_per_unit_dist_norm,0) * ${rentCoef} 
	) / ( 
		coalesce(dist_norm - dist_norm + ${distCoef}, 0 )  + 
		coalesce( year_built_dist_norm - year_built_dist_norm + ${yearBuiltCoef},0 )  + 
		coalesce(sale_price_dist_norm  - sale_price_dist_norm + ${salePriceCoef},0 )  * coalesce(sale_date_dist_norm,0)  +
		coalesce( size_unit_dist_norm - size_unit_dist_norm + ${sizeCoef},0 ) +
		coalesce( size_sqft_dist_norm - size_sqft_dist_norm + ${sizeCoef},0 ) +
		coalesce( rent_per_unit_dist_norm - rent_per_unit_dist_norm + ${rentCoef},0 )
		) as score
	from
	(
	
	select
		t0.id,
		street_address,
		1 - (dist - dist_min) / ( dist_max - dist_min ) dist_norm ,
		1 -  ((year_built_dist + 0.0) - year_built_dist_min) / ( year_built_dist_max - year_built_dist_min ) year_built_dist_norm ,
		1 - ( sale_price_per_unit_dist  - sale_price_per_unit_dist_min ) / ( sale_price_per_unit_dist_max - sale_price_per_unit_dist_min ) sale_price_per_unit_dist_norm ,
		1 - ( (sale_date_dist + 0.0)  - sale_date_dist_min ) / ( sale_date_dist_max - sale_date_dist_min ) sale_date_dist_norm ,
		1 - ( (size_unit_dist + 0.0)  - size_unit_dist_min ) / ( size_unit_dist_max - size_unit_dist_min ) size_unit_dist_norm ,   
		1 - ( (size_sqft_dist + 0.0)  - size_sqft_dist_min ) / ( size_sqft_dist_max - size_sqft_dist_min ) size_sqft_dist_norm ,
		1 - ( (rent_per_unit_dist + 0.0)  - rent_per_unit_dist_min ) / ( rent_per_unit_dist_max - rent_per_unit_dist_min ) rent_per_unit_dist_norm ,		

		1 - ( sale_price_dist  - sale_price_dist_min ) / ( sale_price_dist_max - sale_price_dist_min ) sale_price_dist_norm ,
		sale_price,

		
		TO_CHAR(sale_date, 'mm/dd/yyyy') sale_date,
		year_built,
		dist,
		size_unit,
		sale_price_per_unit,
		rent_per_unit
	
	
	from
		(
	select 
	p."placeId" id,
	p."streetAddress" as street_address,
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
	abs( subj."sizeUnit" - p."sizeUnit" ) as size_unit_dist,
	min( abs(subj."sizeUnit" - p."sizeUnit") ) over() as size_unit_dist_min,
	max( abs(subj."sizeUnit" - p."sizeUnit") ) over( ) as size_unit_dist_max,
	abs( subj."sizeSqft" - p."sizeSqft" ) as size_Sqft_dist,
	min( abs(subj."sizeSqft" - p."sizeSqft") ) over() as size_sqft_dist_min,
	max( abs(subj."sizeSqft" - p."sizeSqft") ) over( ) as size_sqft_dist_max,

	abs( subj."rentPerUnit" - p."rentPerUnit" ) as rent_per_unit_dist,
	min( abs(subj."rentPerUnit" - p."rentPerUnit") ) over() as rent_per_unit_dist_min,
	max( abs(subj."rentPerUnit" - p."rentPerUnit") ) over( ) as rent_per_unit_dist_max,			


	abs(subj."salePrice" - p."salePrice")  as sale_price_dist,
	min( abs(subj."salePrice" - p."salePrice") ) over() as sale_price_dist_min,
	max( abs(subj."salePrice" - p."salePrice") ) over() as sale_price_dist_max,
	p."salePrice" as sale_price,

	subj."saleDate" as subj_sale_date,
	p."saleDate" as sale_date,
	p."sizeUnit" as size_unit,
	p."salePricePerUnit" as sale_price_per_unit,
	p."rentPerUnit" as rent_per_unit
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
		ST_DWithin(p.point::geography,ST_SetSRID(ST_MakePoint(ST_X(subj.point),ST_Y(subj.point)),4326)::geography,${radius} * 1609.34)
	and p."sectorId" = subj."sectorId"
	and p."placeId" != '${placeId}'
	and ( p."performanceObservationId" is not null or (p."saleObservationId" is not null and p."salePrice" is not null))
	) t0
	
	
	) t1
	
	order by score desc
	limit 100
	
	

	`
	console.log( sql )
	pool.query(sql, [],  (err, data) => {

	//console.log( data['rows'] )
	
	res.send(
		edge.render('comps', {data:data['rows']})
		)	
	
	})
}
)



app.listen(port, () => console.log(`Example app listening on port ${port}!`))