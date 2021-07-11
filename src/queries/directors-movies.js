const { sql } = require('slonik')

const getAll = async db => {
    try {
      const { rows: directors_movies } = await db.query(sql`
        SELECT * 
        FROM directors AS d
        INNER JOIN movies AS m
        ON d.id = m.director
      `)
  
      return directors_movies
    } catch (error) {
      console.info('> error: ', error.message)
      return false
    }
  }


const jointWork = async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
      SELECT d.query_name, m.production_budget, distributor
      FROM directors AS d
      INNER JOIN movies AS m
      ON d.id = m.director
      WHERE distributor IS NOT NULL AND d.name ILIKE '% y %'
    `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const moviesPerDirector = async db => {
    try {
      const { rows: directors_movies } = await db.query(sql`
        SELECT d.query_name,COUNT(*)
        FROM directors AS d
        INNER JOIN movies AS m
        ON d.id = m.director
        GROUP BY d.query_name
        ORDER BY d.query_name DESC
    `)
  
      return directors_movies
    } catch (error) {
      console.info('> error: ', error.message)
      return false
    }
  }



const leastVotedMovies = async db => {
    try {
      const { rows: directors_movies } = await db.query(sql`
        SELECT d.query_name, m.title, m.imdb_votes
        FROM directors AS d
        INNER JOIN movies AS m
        ON d.id = m.director
        ORDER BY m.imdb_votes LIMIT 50
    `)
  
      return directors_movies
    } catch (error) {
      console.info('> error: ', error.message)
      return false
    }
}


const ChristopherNolan = async db => {
    try {
      const { rows: directors_movies } = await db.query(sql`
        SELECT d.query_name, distributor
        FROM directors AS d
        INNER JOIN movies AS m
        ON d.id = m.director
        WHERE m.director = 1
    `)
  
      return directors_movies
    } catch (error) {
      console.info('> error: ', error.message)
      return false
    }
}


const topUSgrossDirector = async db => {
    try {
      const { rows: directors_movies } = await db.query(sql`
      SELECT d.name, SUM(m.us_gross)
      FROM directors AS d
      INNER JOIN movies AS m
      ON d.id = m.director
      GROUP BY d.name
      ORDER BY SUM(m.us_gross) DESC LIMIT 1
    `)
  
      return directors_movies
    } catch (error) {
      console.info('> error: ', error.message)
      return false
    }
}


const mostMoviesFrom2000 = async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
    SELECT d.name, COUNT(*)
    FROM directors AS d
    INNER JOIN movies AS m
    ON d.id = m.director
    WHERE m.release_date > '2000-01-01'
    GROUP BY d.name
    ORDER BY COUNT(m.release_date) DESC LIMIT 1
  `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


// 30. Devuelve el nombre, `major_genre` y `rotten_tomatoes_rating` de todos los directores que hayan hecho películas de drama y cuya puntuación en `rotten_tomatoes_rating` sea mayor de 70
const rottenTomatoesDrama = async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
    SELECT d.name, m.major_genre, m.rotten_tomatoes_rating
    FROM directors AS d
    INNER JOIN movies AS m
    ON d.id = m.director
    WHERE m.major_genre ILIKE 'drama' AND m.rotten_tomatoes_rating > 70
    `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

// 31. Devuelve el nombre de los directores australianos que hayan dirigido alguna película antes de 1995
const aussieDirectorsUpTo1995 = async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
    SELECT DISTINCT d.name
    FROM directors AS d
    INNER JOIN movies AS m
    ON d.id = m.director
    WHERE d.nationality ILIKE '%australian%' AND m.release_date < '1995-01-01'
    `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

// 32. Devuelve el nombre de los directores, título, fecha y `mpaa_rating` de las películas cuyo `mpaa_rating` sea `PG-13`
const pg13 = async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
    SELECT d.name, m.title, m.release_date, m.mpaa_rating
    FROM directors AS d
    INNER JOIN movies AS m
    ON d.id = m.director
    WHERE m.mpaa_rating ILIKE 'PG-13'
    `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

// 33. Devuelve el quinto mejor director canadiense que haya obtenido la mejor media de `imdb_rating`
const fifthBestCanadian = async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
    SELECT d.name, AVG(m.imdb_rating)
    FROM directors AS d
    INNER JOIN movies AS m
    ON d.id = m.director
    WHERE d.nationality ~ 'canadiense' AND m.imdb_rating IS NOT NULL
    GROUP BY d.name
    ORDER BY AVG(m.imdb_rating) DESC
    `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

// 34. Devuelve la media de las 20 mejores películas de ficción contemporánea entre 1990 y el 2000 según `rotten_tomatoes_rating` e `imdb_rating`
const twentyBest90s = async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
    SELECT m.title, AVG((coalesce(m.imdb_rating,0)*10) + coalesce(m.rotten_tomatoes_rating,0)) AS average_rating
    FROM directors AS d
    INNER JOIN movies AS m
    ON d.id = m.director
    WHERE m.creative_type ILIKE 'Contemporary fiction' AND m.imdb_rating IS NOT NULL AND m.release_date BETWEEN '1990-01-01' AND '2000-12-31'
    GROUP BY m.title
    ORDER BY average_rating DESC
    LIMIT 20
    `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

// 35. Devuelve los nombre de los directores y las fechas solo en años de películas basadas en juegos y que hayan recaudado menos de `500000$`
const gamesUnder500000= async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
    SELECT d.name, EXTRACT(YEAR FROM m.release_date)
    FROM directors AS d
    INNER JOIN movies AS m
    ON d.id = m.director
    WHERE m.source ILIKE '%game%'
    GROUP BY d.name, m.release_date
    HAVING SUM(m.us_gross + m.worldwide_gross) < 500000
    `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const gamesUnder500000worldwide= async db => {
  try {
    const { rows: directors_movies } = await db.query(sql`
    SELECT d.name, EXTRACT(YEAR FROM m.release_date)
    FROM directors AS d
    INNER JOIN movies AS m
    ON d.id = m.director
    WHERE m.source ILIKE '%game%' AND m.worldwide_gross < 500000
    `)

    return directors_movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

module.exports={
    getAll,
    jointWork,
    moviesPerDirector,
    leastVotedMovies,
    ChristopherNolan,
    topUSgrossDirector,
    mostMoviesFrom2000,
    rottenTomatoesDrama,
    aussieDirectorsUpTo1995,
    pg13,
    fifthBestCanadian,
    twentyBest90s,
    gamesUnder500000,
    gamesUnder500000worldwide,
}