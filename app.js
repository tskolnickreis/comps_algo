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
	"placeId",
	"streetAddress",
	0 "dist",
	"yearBuilt",
	TO_CHAR("saleDate", 'mm/dd/yyyy') as "saleDate",
	"sizeUnit",
	"salePrice",
	"rentPerUnit",
	null score
from
place
where
"placeId" = '${placeId}'

union all
	
	
select
p4."placeId",
p4."streetAddress",
p4."dist",
p4."yearBuilt",
p4."saleDate",
p4."sizeUnit",
p4."salePrice",
p4."rentPerUnit",

round(
	(100* (
		-.000001 +
	coalesce("distNorm",0) *  ${distCoef} +
	coalesce("yearBuiltNormDist",0) *  ${yearBuiltCoef} +
	coalesce("sizeUnitNormDist",0) * ${sizeCoef} +
	coalesce("rentPerUnitNormDist",0) * ${rentCoef} +
	coalesce("salePriceNormDist",0) *   coalesce("saleDateJulianNormDist",0) * ${salePriceCoef}

	)/
	(
		.000001 +
	coalesce("distNorm" - "distNorm" + ${distCoef}, 0 ) +
	coalesce("yearBuiltNormDist" - "yearBuiltNormDist" + ${yearBuiltCoef}, 0 ) +
	coalesce("sizeUnitNormDist" - "sizeUnitNormDist" + ${sizeCoef}, 0 ) +
	coalesce("rentPerUnitNormDist" - "rentPerUnitNormDist" + ${rentCoef}, 0 ) +
	coalesce("salePriceNormDist" - "salePriceNormDist" + ${salePriceCoef}, 0 )	
		
	) )::numeric , 3 ) as score

from
(

select
	p3.*,
	1 - abs("yearBuiltSubjNorm" -  "yearBuiltNorm") as "yearBuiltNormDist",
	1 - abs("salePricePerUnitSubjNorm" -  "salePricePerUnitNorm") as "salePricePerUnitNormDist",
	1 - abs("saleDateJulianSubjNorm" -  "saleDateJulianNorm") as "saleDateJulianNormDist",
	1 - abs("sizeUnitSubjNorm" -  "sizeUnitNorm") as "sizeUnitNormDist",
	1 - abs("rentPerUnitSubjNorm" -  "rentPerUnitNorm") as "rentPerUnitNormDist",
	1 - abs("salePriceSubjNorm" -  "salePriceNorm") as "salePriceNormDist",
	1 - "distRawNorm" as "distNorm"
	

from
(


select
p2.*,

("dist" + 0.0 - "distMin") / ( "distMax" - "distMin" ) "distRawNorm",

("yearBuilt" + 0.0 - "yearBuiltMin") / ( "yearBuiltMax" - "yearBuiltMin" ) "yearBuiltNorm",
("salePricePerUnit" + 0.0 - "salePricePerUnitMin") / ( "salePricePerUnitMax" - "salePricePerUnitMin" ) "salePricePerUnitNorm",
("saleDateJulian" + 0.0 - "saleDateJulianMin") / ( "saleDateJulianMax" - "saleDateJulianMin" ) "saleDateJulianNorm",
("sizeUnit" + 0.0 - "sizeUnitMin") / ( "sizeUnitMax" - "sizeUnitMin" ) "sizeUnitNorm",
("rentPerUnit" + 0.0 - "rentPerUnitMin") / ( "rentPerUnitMax" - "rentPerUnitMin" ) "rentPerUnitNorm",
("salePrice" + 0.0 - "salePriceMin") / ( "salePriceMax" - "salePriceMin" ) "salePriceNorm",

("yearBuiltSubj" + 0.0 - "yearBuiltMin") / ( "yearBuiltMax" - "yearBuiltMin" ) "yearBuiltSubjNorm",
("salePricePerUnitSubj" + 0.0 - "salePricePerUnitMin") / ( "salePricePerUnitMax" - "salePricePerUnitMin" ) "salePricePerUnitSubjNorm",
("saleDateJulianSubj" + 0.0 - "saleDateJulianMin") / ( "saleDateJulianMax" - "saleDateJulianMin" ) "saleDateJulianSubjNorm",
("sizeUnitSubj" + 0.0 - "sizeUnitMin") / ( "sizeUnitMax" - "sizeUnitMin" ) "sizeUnitSubjNorm",
("rentPerUnitSubj" + 0.0 - "rentPerUnitMin") / ( "rentPerUnitMax" - "rentPerUnitMin" ) "rentPerUnitSubjNorm",
("salePriceSubj" + 0.0 - "salePriceMin") / ( "salePriceMax" - "salePriceMin" ) "salePriceSubjNorm"
from
(



select
		p1."placeId",
		p1."streetAddress",
		TO_CHAR(p1."saleDate", 'mm/dd/yyyy') as "saleDate",

		round(p1."dist" ::numeric,3) as dist,
		min(p1."dist" ) over () as "distMin",
		max(p1."dist" ) over () as "distMax",


                p1."yearBuilt",
                min(p1."yearBuilt" ) over () as "yearBuiltMin",
                max(p1."yearBuilt" ) over () as "yearBuiltMax",

                p1."salePricePerUnit",
                min(p1."salePricePerUnit" ) over () as "salePricePerUnitMin",
                max(p1."salePricePerUnit" ) over () as "salePricePerUnitMax",

                p1."saleDateJulian",
                min(p1."saleDateJulian" ) over () as "saleDateJulianMin",
                max(p1."saleDateJulian" ) over () as "saleDateJulianMax",

                p1."sizeUnit",
                min(p1."sizeUnit" ) over () as "sizeUnitMin",
                max(p1."sizeUnit" ) over () as "sizeUnitMax",

                p1."rentPerUnit",
                min(p1."rentPerUnit" ) over () as "rentPerUnitMin",
                max(p1."rentPerUnit" ) over () as "rentPerUnitMax",

                p1."salePrice",
                min(p1."salePrice" ) over () as "salePriceMin",
                max(p1."salePrice" ) over () as "salePriceMax",
	
				"yearBuiltSubj",
				"salePricePerUnitSubj",
				"saleDateJulianSubj",
				"sizeUnitSubj",
				"rentPerUnitSubj",
				"salePriceSubj"
	
	




from
(

        select
			p."placeId",
			ST_Distance( subj.point::geography , p.point::geography  ) * 0.000621371 dist,
			p."streetAddress",
			p."salePricePerUnit" ,
			case when so."isArmsLengthTransaction" then
				p."salePrice"
			else
				null
			end "salePrice",
			case when so."isArmsLengthTransaction" then
				1
			else
				null
			end "isArmsLengthTransaction",
			p."yearBuilt",			
			p."sizeUnit",
			p."rentPerUnit",
			p."saleDate",
			p."saleDateJulian",
			subj."yearBuilt" as "yearBuiltSubj",
			subj."salePricePerUnit" as "salePricePerUnitSubj",
			subj."saleDateJulian" as "saleDateJulianSubj",
			subj."sizeUnit" as "sizeUnitSubj",
			subj."rentPerUnit" as "rentPerUnitSubj",
			subj."salePrice" as "salePriceSubj"

	from
        place p
		left join "saleObservation" so on
			so."saleObservationId" = p."saleObservationId"
        cross join (
                select
					pInner.point,
					pInner."sectorId",
					pInner."yearBuilt",
					pInner."salePricePerUnit",
					pInner."saleDateJulian",
					pInner."sizeUnit",
					pInner."rentPerUnit",
					pInner."salePrice",
					case when soInner."isArmsLengthTransaction" then
						1
					else
						null
					end "isArmsLengthTransaction"

                from
                        place pInner
				left join "saleObservation" soInner on
					soInner."saleObservationId" = pInner."saleObservationId"
                where
                "placeId" = '${placeId}'
                        ) subj
        where
                ST_DWithin(p.point::geography,ST_SetSRID(ST_MakePoint(ST_X(subj.point),ST_Y(subj.point)),4326)::geography,${radius} * 1609.34)
        and p."sectorId" = subj."sectorId"
        and p."placeId" != '${placeId}'
        and ( p."performanceObservationId" is not null or (p."saleObservationId" is not null and p."salePrice" is not null))

	) p1
	
) p2
	order by "salePricePerUnit"
) p3
) p4
order by score desc
	limit 1000
	
	

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