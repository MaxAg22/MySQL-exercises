// Ej1) Especificar en la colección users las siguientes reglas de
// validación: El campo name (requerido) debe ser un string con
// un máximo de 30 caracteres, email (requerido) debe ser un
// string que matchee con la expresión regular: "^(.*)@(.*)\\.(.{2,4})$" ,
// password (requerido) debe ser un string con al menos 50 caracteres.

db.runCommand({
  collMod: "users",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "email", "password"],
      properties: {
        name: {
          bsonType: "string",
          maxLength: 30,
        },
        email: {
          bsonType: "string",
          pattern: "^(.*)@(.*)\\.(.{2,4})$",
        },
        password: {
          bsonType: "string",
          minLength: 50,
        },
      },
    },
  },
});

// Ej2) Obtener metadata de la colección
// users que garantice que las reglas de
// validación fueron correctamente aplicadas.

db.getCollectionInfos({ name: "users" });

// Ej3) Especificar en la colección theaters las siguientes reglas de validación:
// El campo theaterId (requerido) debe ser un int y location (requerido) debe ser un object con:

// un campo address (requerido) que sea un object con campos street1, city, state y zipcode
// todos de tipo string y requeridos

// un campo geo (no requerido) que sea un object con un campo type, con valores posibles
// “Point” o null y coordinates que debe ser una lista de 2 doubles

// Por último, estas reglas de validación no deben prohibir la inserción o actualización de
// documentos que no las cumplan sino que solamente deben advertir.

db.runCommand({
  collMod: "theaters",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["theaterId", "location"],
      properties: {
        theaterId: {
          bsonType: "int",
        },
        location: {
          bsonType: "object",
          required: ["address"],
          properties: {
            address: {
              bsonType: "object",
              required: ["street1", "city", "state", "zipcode"],
              properties: {
                street1: { bsonType: "string" },
                city: { bsonType: "string" },
                state: { bsonType: "string" },
                zipcode: { bsonType: "string" },
              },
            },
            geo: {
              bsonType: "object",
              properties: {
                type: { enum: ["Point", null] },
                coordinates: {
                  bsonType: "array",
                  items: [{ bsonType: "double" }, { bsonType: "double" }],
                  minItems: 2,
                  maxItems: 2,
                },
              },
            },
          },
        },
      },
    },
  },
  validationAction: "warn",
});

// Ej4) Especificar en la colección movies las siguientes reglas de validación:
// El campo title (requerido) es de tipo string, year (requerido) int con mínimo en
// 1900 y máximo en 3000, y que tanto cast, directors, countries, como genres sean
// arrays de strings sin duplicados.
// Hint: Usar el constructor NumberInt() para especificar valores enteros a la hora
// de insertar documentos. Recordar que mongo shell es un intérprete javascript y en
// javascript los literales numéricos son de tipo Number (double).

db.runCommand({
  collMod: "movies",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["title", "year"],
      properties: {
        title: {
          bsonType: "string",
        },
        year: {
          bsonType: "int",
          minimum: NumberInt(1900),
          maximum: NumberInt(3000),
        },
        cast: {
          bsonType: "array",
          items: [{ bsonType: "string" }],
          uniqueItems: true,
        },
        directors: {
          bsonType: "array",
          items: [{ bsonType: "string" }],
          uniqueItems: true,
        },
        countries: {
          bsonType: "array",
          items: [{ bsonType: "string" }],
          uniqueItems: true,
        },
        genres: {
          bsonType: "array",
          items: [{ bsonType: "string" }],
          uniqueItems: true,
        },
      },
    },
  },
});

// Ej5) Crear una colección userProfiles con las siguientes reglas de validación:
// Tenga un campo user_id (requerido) de tipo “objectId”, un campo language (requerido)
// con alguno de los siguientes valores [ “English”, “Spanish”, “Portuguese” ]
//  y un campo favorite_genres (no requerido) que sea un array de strings sin duplicados.

db.createCollection("userProfiles", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "language"],
      properties: {
        user_id: {
          bsonType: "objectId",
        },
        language: {
          enum: ["English", "Spanish", "Portuguese"],
        },
        favorite_genres: {
          bsonType: "array",
          items: [{ bsonType: "string" }],
          uniqueItems: true,
        },
      },
    },
  },
});

// Modelado

// Ej6) Identificar los distintos tipos de relaciones (One-To-One, One-To-Many)
// en las colecciones movies y comments.
// Determinar si se usó documentos anidados o referencias en cada
// relación y justificar la razón.

// Es una relación One-To-Many
// Se utilizo referencias para relacionar comments y movies

// Ej7) Dado el diagrama de la base de datos shop junto con las queries más importantes.

// Queries
// 1) Listar el id, titulo, y precio de los libros y sus categorías de un autor en particular
// 2) Cantidad de libros por categorías
// 3) Listar el nombre y dirección entrega y el monto total (quantity * price) de sus pedidos para
// un order_id dado.

// Debe crear el modelo de datos en mongodb aplicando las estrategias “Modelo de datos anidados” y
// Referencias. El modelo de datos debe permitir responder las queries de manera eficiente.

// Inserte algunos documentos para las colecciones del modelo de datos. Opcionalmente puede
// especificar una regla de validación de esquemas para las colecciones.

// Entidades: books - categories - orders - order_details
// Un libro tiene muchas categorias (one to many) (autor es unico) - Anidado
// Una orden puede tener muchos libros (one to many) - Array de referencias a libros
// order_details representa una orden (one to one) - Referencia

// use shop

db.createCollection("books");
db.books.insertMany([
  {
    book_id: 1,
    title: "Book One",
    author: "Author A",
    price: 10.5,
    categories: [{ category_name: "Fiction" }, { category_name: "Science" }],
  },
  {
    book_id: 2,
    title: "Book Two",
    author: "Author B",
    price: 15.0,
    categories: [{ category_name: "Non-Fiction" }],
  },
]);

db.createCollection("orders");
db.orders.insertOne({
  order_id: 1,
  delivery_name: "Juan Perez",
  delivery_address: "Calle Falsa 123",
  cc_name: "Juan Perez",
  cc_number: "1234-5678-9012-3456",
  cc_expiry: "12/25",
  book_ids: [1, 2],
});

db.createCollection("order_details");
db.order_details.insertMany([
  {
    id: 1,
    book_id: 1,
    quantity: 2,
    order_id: 1,
  },
  {
    id: 2,
    book_id: 2,
    quantity: 1,
    order_id: 1,
  },
]);

// Ej8) Dado el siguiente diagrama que representa los datos de un blog de artículos
// Se pide

// Crear 3 modelos de datos distintos en mongodb aplicando solo las estrategias
// “Modelo de datos anidados” y Referencias (es decir, sin considerar queries).
// Crear un modelo de datos en mongodb aplicando las estrategias “Modelo de datos anidados”
// y Referencias y considerando las siguientes queries.
// 1) Listar título y url, tags y categorías de los artículos dado un user_id
// 2) Listar título, url y comentarios que se realizaron en un rango de fechas.
// 3) Listar nombre y email dado un id de usuario
// Inserte algunos documentos para las colecciones del modelo de datos. Opcionalmente
// puede especificar una regla de validación de esquemas  para las colecciones..

// a)

db.createCollection("users");

db.createCollection("articles");

db.createCollection("comments");

db.users.insertMany([
  { _id: ObjectId(), name: "Alice", email: "alice@example.com" },
  { _id: ObjectId(), name: "Bob", email: "bob@example.com" },
]);

db.articles.insertMany([
  {
    _id: ObjectId(),
    user_id: db.users.findOne({ name: "Alice" })._id,
    title: "Introducción a MongoDB",
    date: ISODate("2025-10-31"),
    text: "MongoDB es una base de datos NoSQL orientada a documentos...",
    url: "https://ejemplo.com/mongodb",
    category: {
      _id: ObjectId(),
      name: "Bases de Datos",
    },
    tags: [
      { _id: ObjectId(), name: "MongoDB" },
      { _id: ObjectId(), name: "NoSQL" },
    ],
  },
  {
    _id: ObjectId(),
    user_id: db.users.findOne({ name: "Bob" })._id,
    title: "Node.js y MongoDB",
    date: ISODate("2025-10-30"),
    text: "Cómo conectar Node.js con MongoDB usando el driver oficial...",
    url: "https://ejemplo.com/node-mongo",
    category: {
      _id: ObjectId(),
      name: "Desarrollo Web",
    },
    tags: [
      { _id: ObjectId(), name: "JavaScript" },
      { _id: ObjectId(), name: "Backend" },
    ],
  },
]);

db.comments.insertMany([
  {
    _id: ObjectId(),
    article_id: db.articles.findOne({ title: "Introducción a MongoDB" })._id,
    user_id: db.users.findOne({ name: "Bob" })._id,
    date: ISODate("2025-10-31"),
    text: "Excelente explicación, muy claro el ejemplo!",
  },
  {
    _id: ObjectId(),
    article_id: db.articles.findOne({ title: "Node.js y MongoDB" })._id,
    user_id: db.users.findOne({ name: "Alice" })._id,
    date: ISODate("2025-10-31"),
    text: "Muy útil para entender cómo conectar ambos.",
  },
]);

// b)
// 1) Listar título y url, tags y categorías de los artículos dado un user_id
//  Modelo anterior, query muy sencilla

// 2) Listar título, url y comentarios que se realizaron en un rango de fechas.
// Por tamaño sigue siendo mejor opción el modelo de antes, podria
// tener un articulo con miles de comentarios. La query seria mas simple si se anida
// pero no es eficiente

// 3) Listar nombre y email dado un id de usuario
// Lo mismo de antes, con el modelo anterior esto es sencillo y rapido
