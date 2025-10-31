// Ej1) Cantidad de cines (theaters) por estado.

db.theaters.aggregate([
  {
    $group: {
      _id: "$location.address.state",
      count: { $sum: 1 },
    },
  },
]);

// Ej2) Cantidad de estados con al menos dos cines (theaters) registrados.

db.theaters.aggregate([
  {
    $group: {
      _id: "$location.address.state",
      count: { $sum: 1 },
    },
  },
  {
    $match: {
      count: { $gte: 2 },
    },
  },
  {
    $count: "numOfStates",
  },
]);

// Ej3) Cantidad de películas dirigidas por "Louis Lumière".
// Se puede responder sin pipeline de agregación, realizar ambas queries.

db.movies.find({ directors: { $in: ["Louis Lumière"] } }).count();

db.movies.aggregate([
  { $unwind: "$directors" },
  {
    $group: {
      _id: "$directors",
      count: { $sum: 1 },
    },
  },
  {
    $match: {
      _id: "Louis Lumière",
    },
  },
]);

// Ej4) Cantidad de películas estrenadas en los años 50 (desde 1950 hasta 1959). Se puede
// responder sin pipeline de agregación, realizar ambas queries.

db.movies
  .find({
    year: { $gte: 1950, $lte: 1959 },
  })
  .count();

db.movies.aggregate([
  {
    $match: { year: { $gte: 1950, $lte: 1959 } },
  },
  {
    $count: "numOfMovies",
  },
]);

// Ej5) Listar los 10 géneros con mayor cantidad de películas (tener en cuenta que las películas
// pueden tener más de un género). Devolver el género y la cantidad de películas. Hint:
// unwind puede ser de utilidad

db.movies.aggregate([
  { $unwind: "$genres" },
  {
    $group: {
      _id: "$genres",
      count: { $sum: 1 },
    },
  },
  { $sort: { count: -1 } },
  { $limit: 10 },
]);

// Ej6) Top 10 de usuarios con mayor cantidad de comentarios, mostrando Nombre, Email y
// Cantidad de Comentarios.

db.users.aggregate([
  {
    $lookup: {
      from: "comments",
      localField: "email",
      foreignField: "email",
      as: "cmts",
    },
  },
  {
    $project: {
      _id: 0,
      name: 1,
      numComments: { $size: "$cmts" },
    },
  },
  {
    $sort: { numComments: -1 },
  },
  { $limit: 10 },
]);

// Ej7) Ratings de IMDB promedio, mínimo y máximo por año de las películas estrenadas en
// los años 80 (desde 1980 hasta 1989), ordenados de mayor a menor por promedio del
// año.

db.movies.aggregate([
  {
    $match: {
      year: { $gte: 1980, $lte: 1989 },
      "imdb.rating": { $type: "double" },
    },
  },
  {
    $group: {
      _id: "$year",
      maxIMDB: { $max: "$imdb.rating" },
      minIMDB: { $min: "$imdb.rating" },
      avgIMDB: { $avg: "$imdb.rating" },
    },
  },
  {
    $sort: { avgIMDB: -1 },
  },
]);

// Ej8) Título, año y cantidad de comentarios de las 10 películas con más comentarios.

db.movies.aggregate([
  {
    $lookup: {
      from: "comments",
      localField: "_id",
      foreignField: "movie_id",
      as: "cmts",
    },
  },
  {
    $project: {
      _id: 0,
      title: 1,
      year: 1,
      numComments: { $size: "$cmts" },
    },
  },
  {
    $sort: { numComments: -1 },
  },
  { $limit: 10 },
]);

// Ej9) Crear una vista con los 5 géneros con mayor cantidad de comentarios, junto con la
// cantidad de comentarios.

db.createView("mostGenreComments1", "movies", [
  { $unwind: "$genres" },
  {
    $lookup: {
      from: "comments",
      localField: "_id",
      foreignField: "movie_id",
      as: "cmts",
    },
  },
  {
    $group: {
      _id: "$genres",
      commentsCount: { $sum: { $size: "$cmts" } },
    },
  },
  { $sort: { commentsCount: -1 } },
  { $limit: 5 },
]);

// Ej10) Listar los actores (cast) que trabajaron en 2 o más películas dirigidas por
// "Jules Bass";.
// Devolver el nombre de estos actores junto con la lista de películas (solo título y año)
// dirigidas por “Jules Bass” en las que trabajaron.
// a. Hint1: addToSet
// b. Hint2: {"name.2": {$exists: true}} permite filtrar arrays con al menos 2
// elementos, entender por qué.
// c. Hint3: Puede que tu solución no use Hint1 ni Hint2 e igualmente sea correcta

db.movies.aggregate([
  {
    $match: { directors: { $in: ["Jules Bass"] } },
  },
  {
    $unwind: "$cast",
  },
  {
    $group: {
      _id: "$cast",
      movies: { $addToSet: { title: "$title", year: "$year" } },
    },
  },
  {
    $match: { "movies.2": { $exists: true } },
  },
]);

// Ej11) Listar los usuarios que realizaron comentarios durante el mismo mes de lanzamiento de
// la película comentada, mostrando Nombre, Email, fecha del comentario, título de la
// película, fecha de lanzamiento. HINT: usar $lookup con multiple condiciones

db.comments.aggregate([
  {
    // lookup entre comentario y pelicula en su fecha, se hardcodea con lastupdated
    $lookup: {
      from: "movies",
      let: { movieId: "$movie_id", commentDate: "$date" },
      pipeline: [
        {
          $addFields: {
            releaseDate: { $toDate: "$lastupdated" },
          },
        },
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$_id", "$$movieId"] },
                {
                  $eq: [{ $year: "$releaseDate" }, { $year: "$$commentDate" }],
                },
                {
                  $eq: [
                    { $month: "$releaseDate" },
                    { $month: "$$commentDate" },
                  ],
                },
              ],
            },
          },
        },
      ],
      as: "movieInfo",
    },
  },
  {
    // lookup entre comentario y el usuario que la creo
    $lookup: {
      from: "users",
      localField: "email",
      foreignField: "email",
      as: "userInfo",
    },
  },
  {
    // imprimo la info del usuario, del comentario y de la pelicula asociada
    $project: {
      _id: 0,
      userName: "$userInfo.name",
      userEmail: "$userInfo.email",
      commentDate: "$date",
      movieTitle: "$movieInfo.title",
      movieDate: "$movieInfo.lastupdated",
    },
  },
]);

// Ej12) Listar el id y nombre de los restaurantes junto con su puntuación máxima, mínima y la
// suma total. Se puede asumir que el restaurant_id es único.
// a. Resolver con $group y accumulators.
// b. Resolver con expresiones sobre arreglos (por ejemplo, $sum) pero sin $group.
// c. Resolver como en el punto b) pero usar $reduce para calcular la puntuación
// total.
// d. Resolver con find.

// a
db.restaurants.aggregate([
  { $unwind: "$grades" },
  {
    $group: {
      _id: "$restaurant_id",
      name: { $first: "$name" },
      maxScore: { $max: "$grades.score" },
      minScore: { $min: "$grades.score" },
      sumScore: { $sum: "$grades.score" },
    },
  },
]);

// b
db.restaurants.aggregate([
  {
    $project: {
      _id: 0,
      restaurant_id: 1,
      name: 1,
      maxScore: { $max: "$grades.score" },
      minScore: { $min: "$grades.score" },
      sumScore: { $sum: "$grades.score" },
    },
  },
]);

// c
db.restaurants.aggregate([
  {
    $project: {
      _id: 0,
      restaurant_id: 1,
      name: 1,
      sumScore: {
        $reduce: {
          input: "$grades",
          initialValue: 0,
          in: { $add: ["$$value", "$$this.score"] },
        },
      },
    },
  },
]);

// d
// NOSE

// 13. Actualizar los datos de los restaurantes añadiendo dos campos nuevos.
// a. "average_score" con la puntuación promedio
// b. "grade": con "A" si "average_score" está entre 0 y 13,
// con "B" si "average_score" está entre 14 y 27
// con "C" si "average_score" es mayor o igual a 28

// Se debe actualizar con una sola query.
// a. HINT1. Se puede usar pipeline de agregación con la operación update
// b. HINT2. El operador $switch o $cond pueden ser de ayuda.

db.restaurants.aggregate([
  {
    $addFields: {
      avarage_score: { $avg: "$grades.score" },
      grade: {
        $cond: [
          {
            $and: [
              { $gte: [{ $avg: "$grades.score" }, 0] },
              { $lte: [{ $avg: "$grades.score" }, 13] },
            ],
          },
          "A",
          {
            $cond: [
              {
                $and: [
                  { $gte: [{ $avg: "$grades.score" }, 14] },
                  { $lte: [{ $avg: "$grades.score" }, 27] },
                ],
              },
              "B",
              {
                $cond: [
                  {
                    $and: [{ $gte: [{ $avg: "$grades.score" }, 28] }, {}],
                  },
                  "C",
                  {},
                ],
              },
            ],
          },
        ],
      },
    },
  },
]);

db.restaurants.updateMany({}, [
  {
    $set: {
      average_score: { $avg: "$grades.score" },
      grade: {
        $switch: {
          branches: [
            { case: { $lte: [{ $avg: "$grades.score" }, 13] }, then: "A" },
            { case: { $lte: [{ $avg: "$grades.score" }, 27] }, then: "B" },
            { case: { $lte: [{ $avg: "$grades.score" }, 40] }, then: "C" },
          ],
          default: "D",
        },
      },
    },
  },
]);
