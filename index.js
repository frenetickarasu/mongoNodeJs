const MongoClient = require('mongodb').MongoClient
const URL = 'mongodb://localhost:27017'

async function createData (db) {
	try {
		let data = {
			a: "cualquier cosa",
			b: [4,3,2,1]
		}
		let res = await db.collection('example').save(data)
		console.log(res)
	} catch (error) {
		console.log('no se pudo crear')
	}
}

async function updateData (db) {
	try {
		let update = {$set: {b: [55,10,8]}}
		let res = db.collection('example').findOneAndUpdate({
			a: "cualquier cosa"
		}, update, {new:true}).toArray()
		console.log(res);
	} catch (error) {
		console.log(`HUBO UN ERROR D: ${error}` )
	}
}


async function findData (db) {
	try {
		const data = await db.collection('example').find({},{
			limit: 5,
			projection: {b:1}
		}).toArray()
		console.log(data)
	} catch (error) {
		console.log("oh oh");
	}
}

async function aggregationsShow(db) {
	// try {
	// 	let res = await db.collection('movieDetails').aggregate([
	// 		{$sample: 10,
	// 		{$project: {title:1,actors:1, "imdb.rating":1}},
	// 		{$addFields: {"rating": "$imdb.rating"}}
	// 	]).toArray()
	// 	console.log(res)
	// } catch (error) {
	// 	console.log(`se murióoooo ${error}`)
	// }
	// try {
	// 	let res = await db.collection('movieDetails').aggregate([
	// 		{$match: {countries: "Spain" }},
	// 		{$project: {
	// 			actors: 1
	// 		}},
	// 		{$unwind: "$actors"},
	// 		{$group: {
	// 			_id: "$actors",
	// 			totalMovies: {$sum:1}
	// 		}}
	// 	]).toArray()
	// 	console.log(`total de actores: ${res.length}`)
	// 	console.log(res)
	// } catch (error) {
	// 	console.log('error')
	// }
	// 
	// try {
	// 	let res = await db.collection('movieDetails').aggregate([
	// 		{$match: {countries: "USA", released: {$ne:null}}},
	// 		{$project: {title:1,year: {$year: "$released"}}},
	// 		{$bucket:{
	// 			groupBy:"$year",
	// 			boundaries: [1979,2000,2001,2010,2011,2012,2013],
	// 			default:"other",
	// 			output: {
	// 				total: {$sum:1},
	// 				movies: {
	// 					$push: {title:"$title"}
	// 				}
	// 			}
	// 		}}
	// 	]).toArray()
	// 	//console.log(`total de actores: ${res.length}`)
	// 	console.log(JSON.stringify(res))
	// } catch (error) {
	// 	console.log('error'+error)
	// }
	// try {
	// 	let res = await db.collection('movieDetails').aggregate([
	// 		{$match: {countries: "USA", released: {$ne:null}}},
	// 		{$project: {title:1,year: {$year: "$released"}}},
	// 		{$sortByCount: "$year"}
	// 		// {$bucket:{
	// 		// 	groupBy:"$year",
	// 		// 	boundaries: [1979,2000,2001,2010,2011,2012,2013],
	// 		// 	default:"other",
	// 		// 	output: {
	// 		// 		total: {$sum:1},
	// 		// 		movies: {
	// 		// 			$push: {title:"$title"}
	// 		// 		}
	// 		// 	}
	// 		// }}
	// 	]).toArray()
	// 	//console.log(`total de actores: ${res.length}`)
	// 	console.log(JSON.stringify(res))
	// } catch (error) {
	// 	console.log('error'+error)
	// }
	//EJERCICIO 2 OK
	// try {
	// 	let res = await db.collection('movieDetails').aggregate([
	// 		{$match: {genres: "Romance"}},
	// 		{$project: {title:1, actors:1, "imdb.rating":1}},
	// 		{$unwind: "$actors"},
	// 		{$group: {
	// 			_id: "$actors",
	// 			totalMovies: {$sum:1},
	// 			promedio: {$avg:"$imdb.rating"}
	// 		}},
	
	// 		{$sort:{totalMovies:-1}},
	// 		{$limit:3},
	// 		{$project: 
	// 			{actor:"$_id", numFilms: "$totalMovies", average: "$promedio", _id:0}
	// 		}

	// 	]).toArray()
	// 	console.log(JSON.stringify(res))
	// } catch (error) {
	// 	console.log('error'+error)
	// }
//EJERCICIO 3
try {
	let res = await db.collection('people').aggregate([
		{$match: {birthday: {$ne:null}, last_name:/^S/}},
		{$project: {last_name:1, first_name:1,job:1,"address.city":1, year: {$year: "$birthday"}}},
		// {$unwind: "$actors"},
		// {$group: {
		// 	_id: "$actors",
		// 	totalMovies: {$sum:1},
		// 	promedio: {$avg:"$imdb.rating"}
		// }},
		//{$limit:100},
		{$bucket:{
			groupBy:"$year",
			boundaries: [2010,2011,2012],
			default:"other",
			output: {
				count: {$sum:1},
				people: {
					$push: {
						full_name: {$concat: [ "$first_name", " ", "$last_name"]},
						job:"$job",
						city:"$address.city",
						year:"$birthday"
					}
				}
			}
		}}



		// {$sort:{totalMovies:-1}},
		// {$limit:3},
		// {$project: 
		// 	{actor:"$_id", numFilms: "$totalMovies", average: "$promedio", _id:0}
		// }

	]).toArray()
	console.log(JSON.stringify(res))
} catch (error) {
	console.log('error'+error)
}

}



async function init () {
  const dbName = 'video'
  const client = new MongoClient(URL, {useNewUrlParser: true})
  try {
		await client.connect()
		console.log("waiting...")
		const db = client.db(dbName)
		
		//createData(db)
		//updateData(db)
		//findData(db)
		aggregationsShow(db)
		
  } catch (error) {
    throw new Error(`Unable to connect ${error}`)
	}
}


	
init()
