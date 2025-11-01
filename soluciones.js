// Ej1) Calcular el rating promedio por país. Listar el país, rating
// promedio, y cantidad de
// rating. Listar en orden descendente por rating promedio. Usar el campo
// “review_scores.review_scores_rating” para calcular el rating promedio.}

db.listingsAndReviews.aggregate([
  {
    $group: {
      _id: "$address.country",
      avgRating: { $avg: "$review_scores.review_scores_rating" },
      Reviews: { $sum: 1 }, // Esto esta mal, creo
    },
  },
  {
    $sort: { avgRating: -1 },
  },
]);

// Ej2) Listar los 20 alojamientos que tienen las reviews más recientes.
// Listar el id, nombre,
// fecha de la última review, y cantidad de reviews del alojamiento. Listar en orden
// descendente por cantidad de reviews.
// HINT: $first pueden ser de utilidad.

db.listingsAndReviews.aggregate([
  { $unwind: "$reviews" },
  {
    $group: {
      _id: "$_id",
      data: { $last: "$reviews" },
      date: { $last: "$reviews.date" },
      count: { $sum: 1 },
    },
  },
  {
    $project: {
      _id: "$_id",
      name: "$name",
      data: "$data",
      date_time: "$date",
      count: "$count",
    },
  },
  {
    $sort: {
      date: -1,
    },
  },
  {
    $limit: 20,
  },
]);

// Ej3) Crear la vista “top10_most_common_amenities” con información de los
// 10 amenities
// que aparecen con más frecuencia. El resultado debe mostrar el amenity y la
// cantidad de veces que aparece cada amenity.

db.createView("top10_most_common_amentities", "listingsAndReviews", [
  {
    $unwind: "$amenities",
  },
  {
    $group: {
      _id: "$amenities",
      amenitiesCount: { $sum: 1 },
    },
  },
  {
    $sort: { amenitiesCount: -1 },
  },
  { $limit: 10 },
]);

// Ej4) Actualizar los alojamientos de Brazil que tengan un rating global
// (“review_scores.review_scores_rating”) asignado, agregando el campo
// "quality_label" que clasifique el alojamiento como “High” (si el rating global es mayor
// o igual a 90), “Medium” (si el rating global es mayor o igual a 70), “Low” (valor por
// defecto) calidad..
// HINTS:
// (i) para actualizar se puede usar pipeline de agregación.
// (ii) El operador $cond o $switch pueden ser de utilidad.

db.listingsAndReviews.aggregate([
  {
    $match: {
      $and: [
        { "address.country": { $eq: "Brazil" } },
        { "review_scores.review_scores_rating": { $exists: true } },
      ],
    },
  },
  {
    $addFields: {
      quality_label: {
        $switch: {
          branches: [
            {
              case: { $gte: ["$review_scores.review_scores_rating", 90] },
              then: "High",
            },
            {
              case: { $gte: ["$review_scores.review_scores_rating", 70] },
              then: "Medium",
            },
          ],
          default: "Low",
        },
      },
    },
  },
]);

// Ej5) (a) Especificar reglas de validación en la colección listingsAndReviews
// a los siguientes campos requeridos: name, address, amenities, review_scores, and
// reviews ( y todos sus campos anidados). Inferir los tipos y otras restricciones que
// considere adecuados para especificar las reglas a partir de los documentos de la
// colección.
// (b) Testear la regla de validación generando dos casos de fallas en la regla de
// validación y un caso de éxito en la regla de validación. Aclarar en la entrega cuales
// son los casos y por qué fallan y cuales cumplen la regla de validación. Los casos no
// deben ser triviales, es decir los ejemplos deben contener todos los campos
// especificados en la regla.

db.runCommand({
  collMod: "listingsAndReviews",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "address", "amenities", "review_scores", "reviews"],
      properties: {
        name: {
          bsonType: "string",
          maxLength: 30,
        },
        address: {
          bsonType: "object",
          properties: {
            street: {
              bsonType: "string",
            },
            suburb: {
              bsonType: "string",
            },
            government_area: {
              bsonType: "string",
            },
            market: {
              bsonType: "string",
            },
            country: {
              bsonType: "string",
            },
            country_code: {
              bsonType: "string",
            },
            location: {
              bsonType: "object",
              required: ["type", "coordinates", "is_location_exact"],
              properties: {
                type: {
                  bsonType: "string",
                },
                coordinates: {
                  bsonType: "array",
                  items: [{ bsonType: "double" }, { bsonType: "double" }],
                  minItems: 2,
                  maxItems: 2,
                },
                is_location_exact: {
                  bsonType: "string",
                },
              },
            },
          },
        },
        amenities: {
          bsonType: "array",
          items: { bsonType: "string" },
        },
        review_scores: {
          bsonType: "object",
          required: [
            "review_scores_accuracy",
            "review_scores_cleanliness",
            "review_scores_checkin",
            "review_scores_communication",
            "review_scores_location",
            "review_scores_value",
            "review_scores_rating",
          ],
          properties: {
            review_scores_accuracy: {
              bsonType: "int",
              minimum: NumberInt(0),
              maximum: NumberInt(10),
            },
            review_scores_cleanliness: {
              bsonType: "int",
              minimum: NumberInt(0),
              maximum: NumberInt(10),
            },
            review_scores_checkin: {
              bsonType: "int",
              minimum: NumberInt(0),
              maximum: NumberInt(10),
            },
            review_scores_communication: {
              bsonType: "int",
              minimum: NumberInt(0),
              maximum: NumberInt(10),
            },
            review_scores_location: {
              bsonType: "int",
              minimum: NumberInt(0),
              maximum: NumberInt(10),
            },
            review_scores_value: {
              bsonType: "int",
              minimum: NumberInt(0),
              maximum: NumberInt(10),
            },
            review_scores_rating: {
              bsonType: "int",
              minimum: NumberInt(0),
              maximum: NumberInt(100),
            },
          },
        },
        reviews: {
          bsonType: "array",
          items: [
            {
              bsonType: ["object"],
              required: [
                "_id",
                "date",
                "listing_id",
                "reviewer_id",
                "reviewer_name",
                "comments",
              ],
              properties: {
                _id: {
                  bsonType: "int",
                },
                date: {
                  bsonType: "date",
                },
                listing_id: {
                  bsonType: "int",
                },
                reviewer_id: {
                  bsonType: "int",
                },
                reviewer_name: {
                  bsonType: "string",
                },
                comments: {
                  bsonType: "string",
                },
              },
            },
          ],
        },
      },
    },
  },
});

// b)

// db.getCollectionInfos({name: "listingsAndReviews"})
db.listingsAndReviews.updateOne(
  { _id: "10006546" },
  {
    $set: { name: "este es un texto muy largo y no entra" },
  }
);

// Mensaje recibido:
// MongoServerError: Plan executor error during update ::
// caused by :: Document failed validation
// Falla porque el maximo largo de name es de 30.
db.listingsAndReviews.updateOne(
  { _id: "10006546" },
  {
    $addToSet: { amenities: 1290090 },
  }
);

// amenities no puede ser de tipo entero, es de tipo string
db.listingsAndReviews.updateOne(
  { _id: "10006546" },
  {
    $set: { name: "jose" },
  }
);

//  caso exitoso
db.listingsAndReviews.updateOne(
  {
    _id: "10006546",
    listing_url: "https://www.airbnb.com/rooms/10006546",
    name: "Ribeira Charming Duplex",
    summary:
      "Fantastic duplex apartment with three bedrooms, located in the historic area of Porto, Ribeira (Cube) - UNESCO World Heritage Site. Centenary building fully rehabilitated, without losing their original character.",
    space:
      "Privileged views of the Douro River and Ribeira square, our apartment offers the perfect conditions to discover the history and the charm of Porto. Apartment comfortable, charming, romantic and cozy in the heart of Ribeira. Within walking distance of all the most emblematic places of the city of Porto. The apartment is fully equipped to host 8 people, with cooker, oven, washing machine, dishwasher, microwave, coffee machine (Nespresso) and kettle. The apartment is located in a very typical area of the city that allows to cross with the most picturesque population of the city, welcoming, genuine and happy people that fills the streets with his outspoken speech and contagious with your sincere generosity, wrapped in a only parochial spirit.",
    description:
      "Fantastic duplex apartment with three bedrooms, located in the historic area of Porto, Ribeira (Cube) - UNESCO World Heritage Site. Centenary building fully rehabilitated, without losing their original character. Privileged views of the Douro River and Ribeira square, our apartment offers the perfect conditions to discover the history and the charm of Porto. Apartment comfortable, charming, romantic and cozy in the heart of Ribeira. Within walking distance of all the most emblematic places of the city of Porto. The apartment is fully equipped to host 8 people, with cooker, oven, washing machine, dishwasher, microwave, coffee machine (Nespresso) and kettle. The apartment is located in a very typical area of the city that allows to cross with the most picturesque population of the city, welcoming, genuine and happy people that fills the streets with his outspoken speech and contagious with your sincere generosity, wrapped in a only parochial spirit. We are always available to help guests",
    neighborhood_overview:
      "In the neighborhood of the river, you can find several restaurants as varied flavors, but without forgetting the so traditional northern food. You can also find several bars and pubs to unwind after a day's visit to the magnificent Port. To enjoy the Douro River can board the boats that daily make the ride of six bridges. You can also embark towards Régua, Barca d'Alva, Pinhão, etc and enjoy the Douro Wine Region, World Heritage of Humanity. The Infante's house is a few meters and no doubt it deserves a visit. They abound grocery stores, bakeries, etc. to make your meals. Souvenir shop, wine cellars, etc. to bring some souvenirs.",
    notes:
      "Lose yourself in the narrow streets and staircases zone, have lunch in pubs and typical restaurants, and find the renovated cafes and shops in town. If you like exercise, rent a bicycle in the area and ride along the river to the sea, where it will enter beautiful beaches and terraces for everyone. The area is safe, find the bus stops 1min and metro line 5min. The bustling nightlife is a 10 min walk, where the streets are filled with people and entertainment for all. But Porto is much more than the historical center, here is modern museums, concert halls, clean and cared for beaches and surf all year round. Walk through the Ponte D. Luis and visit the different Caves of Port wine, where you will enjoy the famous port wine. Porto is a spoken city everywhere in the world as the best to be visited and savored by all ... natural beauty, culture, tradition, river, sea, beach, single people, typical food, and we are among those who best receive tourists, confirm! Come visit us and feel at ho",
    transit:
      "Transport: • Metro station and S. Bento railway 5min; • Bus stop a 50 meters; • Lift Guindais (Funicular) 50 meters; • Tuc Tuc-to get around the city; • Buses tourist; • Cycling through the marginal drive; • Cable car in Gaia, overlooking the Port (just cross the bridge).",
    access:
      'We are always available to help guests. The house is fully available to guests. We are always ready to assist guests. when possible we pick the guests at the airport.  This service transfer have a cost per person. We will also have service "meal at home" with a diverse menu and the taste of each. Enjoy the moment!',
    interaction: "Cot - 10 € / night Dog - € 7,5 / night",
    house_rules: "Make the house your home...",
    property_type: "House",
    room_type: "Entire home/apt",
    bed_type: "Real Bed",
    minimum_nights: "2",
    maximum_nights: "30",
    cancellation_policy: "moderate",
    last_scraped: ISODate("2019-02-16T05:00:00.000Z"),
    calendar_last_scraped: ISODate("2019-02-16T05:00:00.000Z"),
    first_review: ISODate("2016-01-03T05:00:00.000Z"),
    last_review: ISODate("2019-01-20T05:00:00.000Z"),
    accommodates: 8,
    bedrooms: 3,
    beds: 5,
    number_of_reviews: 51,
    bathrooms: Decimal128("1.0"),
    amenities: ["TV", "Cable TV", "Wifi"],
    price: Decimal128("80.00"),
    security_deposit: Decimal128("200.00"),
    cleaning_fee: Decimal128("35.00"),
    extra_people: Decimal128("15.00"),
    guests_included: Decimal128("6"),
    images: {
      thumbnail_url: "",
      medium_url: "",
      picture_url:
        "https://a0.muscache.com/im/pictures/e83e702f-ef49-40fb-8fa0-6512d7e26e9b.jpg?aki_policy=large",
      xl_picture_url: "",
    },
    host: {
      host_id: "51399391",
      host_url: "https://www.airbnb.com/users/show/51399391",
      host_name: "Ana&Gonçalo",
      host_location: "Porto, Porto District, Portugal",
      host_about:
        "Gostamos de passear, de viajar, de conhecer pessoas e locais novos, gostamos de desporto e animais! Vivemos na cidade mais linda do mundo!!!",
      host_response_time: "within an hour",
      host_thumbnail_url:
        "https://a0.muscache.com/im/pictures/fab79f25-2e10-4f0f-9711-663cb69dc7d8.jpg?aki_policy=profile_small",
      host_picture_url:
        "https://a0.muscache.com/im/pictures/fab79f25-2e10-4f0f-9711-663cb69dc7d8.jpg?aki_policy=profile_x_medium",
      host_neighbourhood: "",
      host_response_rate: 100,
      host_is_superhost: false,
      host_has_profile_pic: true,
      host_identity_verified: true,
      host_listings_count: 3,
      host_total_listings_count: 3,
      host_verifications: [
        "email",
        "phone",
        "reviews",
        "jumio",
        "offline_government_id",
        "government_id",
      ],
    },
    address: {
      street: "Porto, Porto, Portugal",
      suburb: "",
      government_area: "Cedofeita, Ildefonso, Sé, Miragaia, Nicolau, Vitória",
      market: "Porto",
      country: "Portugal",
      country_code: "PT",
      location: {
        type: "Point",
        coordinates: [-8.61308, 41.1413],
        is_location_exact: false,
      },
    },
    availability: {
      availability_30: 28,
      availability_60: 47,
      availability_90: 74,
      availability_365: 239,
    },
    review_scores: {
      review_scores_accuracy: 9,
      review_scores_cleanliness: 9,
      review_scores_checkin: 10,
      review_scores_communication: 10,
      review_scores_location: 10,
      review_scores_value: 9,
      review_scores_rating: 89,
    },
    reviews: [
      {
        _id: "58663741",
        date: ISODate("2016-01-03T05:00:00.000Z"),
        listing_id: "10006546",
        reviewer_id: "51483096",
        reviewer_name: "Cátia",
        comments:
          "A casa da Ana e do Gonçalo foram o local escolhido para a passagem de ano com um grupo de amigos. Fomos super bem recebidos com uma grande simpatia e predisposição a ajudar com qualquer coisa que fosse necessário.\r\n" +
          "A casa era ainda melhor do que parecia nas fotos, totalmente equipada, com mantas, aquecedor e tudo o que pudessemos precisar.\r\n" +
          "A localização não podia ser melhor! Não há melhor do que acordar de manhã e ao virar da esquina estar a ribeira do Porto.",
      },
    ],
  },
  {
    $set: { name: "jose" },
  }
);
