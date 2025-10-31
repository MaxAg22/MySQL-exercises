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
// &quot;Jules Bass&quot;.
// Devolver el nombre de estos actores junto con la lista de películas (solo título y año)
// dirigidas por “Jules Bass” en las que trabajaron.
// a. Hint1: addToSet
// b. Hint2: {&#39;name.2&#39;: {$exists: true}} permite filtrar arrays con al menos 2
// elementos, entender por qué.
// c. Hint3: Puede que tu solución no use Hint1 ni Hint2 e igualmente sea correcta
