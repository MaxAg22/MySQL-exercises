// Comando para montar una base de datos
// mongorestore --uri="mongodb://localhost:27017" --db=mflix --gzip mflix

// Ej1) Insertar 5 nuevos usuarios en la colección users. Para cada nuevo usuario creado, 
// insertar al menos un comentario realizado por el usuario en la colección comments.

const nuevosUsuarios = [
{
    name: "Ana López",
    email: "ana.lopez@example.com",
    password: "password123"
},
{

    name: "Carlos García",
    email: "carlos.garcia@example.com",
    password: "password456"
},
{

    name: "Sofía Martínez",
    email: "sofia.martinez@example.com",
    password: "password789"
},
{

    name: "David Rodríguez",
    email: "david.rodriguez@example.com",
    password: "password101"
},
{

    name: "Elena Fernández",
    email: "elena.fernandez@example.com",
    password: "password112"
}
];

const resultadoUsers = db.users.insertMany(nuevosUsuarios);

print("Usuarios insertados con los siguientes IDs:");
printjson(resultadoUsers.insertedIds);

const nuevosComentarios = [

{

name: "Ana López", 
email: "ana.lopez@example.com", 
movie_id: ObjectId("573a139af29313caabcf0894"), 
text: "¡Qué gran película! La mejor que he visto este año.",
date: new Date(),
user_id: resultadoUsers.insertedIds[0] 
},

{

name: "Carlos García",
email: "carlos.garcia@example.com",
movie_id: ObjectId("573a139af29313caabcf0894"),
text: "No estoy de acuerdo, me pareció un poco lenta.",
date: new Date(),
user_id: resultadoUsers.insertedIds[1]
},

{

name: "Sofía Martínez",
email: "sofia.martinez@example.com",
movie_id: ObjectId("573a139bf29313caabcf3d23"),
text: "Me encantó la fotografía de esta película.",
date: new Date(),
user_id: resultadoUsers.insertedIds[2] 
},

{

name: "David Rodríguez",
email: "david.rodriguez@example.com",
movie_id: ObjectId("573a139bf29313caabcf3d23"),
text: "El final fue inesperado. ¡Tengo que verla de nuevo!",
date: new Date(),
user_id: resultadoUsers.insertedIds[3]
},

{

name: "Elena Fernández",
email: "elena.fernandez@example.com",
movie_id: ObjectId("573a13a0f29313caabd048a2"),
text: "Simplemente espectacular.",
date: new Date(),
user_id: resultadoUsers.insertedIds[4]
}

];

print("\nSe han insertado " + resultadoComments.insertedCount + " comentarios."); // me da undefined

// Ej2) Listar el título, año, actores (cast), directores y rating de las 10 películas con mayor 
// rating (“imdb.rating”) de la década del 90. ¿Cuál es el valor del rating de la película 
// que tiene mayor rating? (Hint: Chequear que el valor de “imdb.rating” sea de tipo “double”).

db.movies.find(
    { "imdb.rating": { $type: "double" } },
    {
        "_id": 0,
        "title": 1,
        "year": 1,
        "cast": 1,
        "directors": 1,
        "imdb.rating": 1
    }
).sort(
    {
        "imdb.rating": -1
    }
).skip(
0
).limit(10)

// Ej3) Listar el nombre, email, texto y fecha de los comentarios que la película con id (movie_id) 
// ObjectId("573a1399f29313caabcee886") recibió entre los años 2014 y 2016 inclusive. 
// Listar ordenados por fecha. 

db.comments.find(
    {
        "movie_id": ObjectId("573a1399f29313caabcee886"),
        "date": {   
                    $gte: ISODate("2014-01-01T00:00:00Z"),
                    $lte: ISODate("2016-12-31T23:59:59Z") 
                },
    },
    { 
        "_id": 0,
        "name": 1,
        "email": 1,
        "text": 1,
        "year": { $year: "$date" }
    }
).sort(
    {"date": 1}
)

// Aggregation Framework: $year, $month, $dayOfMonth.

// Escribir una nueva consulta (modificando la anterior) 
// para responder ¿Cuántos comentarios recibió?

db.comments.find(
    {
        "movie_id": ObjectId("573a1399f29313caabcee886"),
        "date": {   
                    $gte: ISODate("2014-01-01T00:00:00Z"),
                    $lte: ISODate("2016-12-31T23:59:59Z") 
                },
    },
    { 
        "_id": 0,
        "name": 1,
        "email": 1,
        "text": 1,
        "year": { $year: "$date" }
    }
).sort(
    {"date": 1}
).count()

// Ej4) Listar el nombre, id de la película, texto y fecha de los 3 
// comentarios más recientes realizados por el usuario con email patricia_good@fakegmail.com. 

db.comments.find(
    { "email": "patricia_good@fakegmail.com" },
    { 
        "_id": 0,
        "name": 1,
        "movie_id": 1,
        "text": 1
    }
).sort(
    { "date": -1 }
).limit(3);

// Ej5) Listar el título, idiomas (languages), géneros, fecha de lanzamiento (released) y 
// número de votos (“imdb.votes”) de las películas de géneros Drama y Action 
// (la película puede tener otros géneros adicionales), que solo están disponibles 
// en un único idioma y por último tengan un rating (“imdb.rating”) 
// mayor a 9 o bien tengan una duración (runtime) de al menos 180 minutos. 
// Listar ordenados por fecha de lanzamiento y número de votos.


db.movies.find(
    {   
        $and: [
            { "genres": { $all: ["Drama", "Action"] } },
            { "imdb.rating": { $type: "double" } },
            { $or: [
                { "imdb.rating": { $gt: 9 } },
                { "runtime": { $gte: 180 } }
            ]}
        ]
    },
    {
        "_id": 0,
        "title": 1,
        "genres": 1,
        "year": 1,
        "imdb.votes": 1,

        "runtime": 1,
        "imdb.rating": 1,
    }
).sort(
    {
        "year": -1, // Las más recientes primero
        "imdb.votes": -1 // La que más votos tenga
    } 
)

// Ej6) Listar el id del teatro (theaterId), estado (“location.address.state”), 
// ciudad (“location.address.city”), y coordenadas (“location.geo.coordinates”) 
// de los teatros que se encuentran en algunos de los estados "CA", "NY", "TX" y 
// el nombre de la ciudades comienza con una ‘F’. Listar ordenados por estado y ciudad.

db.theaters.find(
    {
        $and: [
            { "location.address.state": { $in: ["CA", "NY", "TX"] } },
            { "location.address.city": /^F/}
        ]
    },
    {
        "_id": 0,
        "theaterId": 1,
        "location.address.state": 1,
        "location.address.city": 1,
        "location.geo.coordinates": 1
    }
).sort(
    {
        "location.address.state": 1,
        "location.address.city": 1
    }
)

// Ej7) Actualizar los valores de los campos texto (text) y fecha (date) 
// del comentario cuyo id es ObjectId("5b72236520a3277c015b3b73") a 
// "mi mejor comentario" y fecha actual respectivamente.

db.comments.updateOne(
    { "_id": ObjectId("5b72236520a3277c015b3b73") },
    {
        $set: {
            "text": "mi mejor comentario",
            "date": new Date()
        }
    }
)

db.comments.find(
    { "_id": ObjectId("5b72236520a3277c015b3b73") },
)

// Ej8) Actualizar el valor de la contraseña del usuario cuyo email es 
// joel.macdonel@fakegmail.com a "some password". 
// La misma consulta debe poder insertar un nuevo usuario en caso que el usuario no exista. 
// Ejecute la consulta dos veces. ¿Qué operación se realiza en cada caso?  (Hint: usar upserts).

db.users.updateOne(
    { "email": "joel.macdonel@fakegmail.com" },
    {
        $set: { "password": "some password" },
        $setOnInsert: { "name": "Joel", "email": "joel.macdonel@fakegmail.com" }
    },
    { upsert: true }
)

db.users.find(
    { "email": "joel.macdonel@fakegmail.com" },
)

// Ej9) Remover todos los comentarios realizados por el usuario cuyo email 
// es victor_patel@fakegmail.com durante el año 1980.

db.comments.deleteMany(
    {
        $and: [
            { "email": "victor_patel@fakegmail.com" },
            { date: {
                $gte: ISODate("1980-01-01T00:00:00Z"),
                $lte: ISODate("1980-12-31T23:59:59Z")
                } 
            }
        ]
    }
)

// { acknowledged: true, deletedCount: 21 }

