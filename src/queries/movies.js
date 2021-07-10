const { sql } = require('slonik')

const getAll = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT * FROM movies
    `)

    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const nonNull = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT title FROM movies WHERE title IS NOT NULL
    `)

    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const lowBudget = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT title, production_budget, distributor
      FROM movies WHERE production_budget < 500000
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const mostExpensive = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT title, major_genre, production_budget
      FROM movies
      WHERE production_budget IS NOT NULL   
      ORDER BY production_budget DESC
      LIMIT 10
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const remakes = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT title, source
      FROM movies
      WHERE source ILIKE 'remake'
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const imdbRating = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT title, distributor, imdb_rating
      FROM movies
      WHERE imdb_rating IS NOT NULL
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const rottenTomatoes = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT title, rotten_tomatoes_rating
      FROM movies
      ORDER BY rotten_tomatoes_rating LIMIT 100
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const imdbVotings = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT title, major_genre, imdb_rating, imdb_votes
      FROM movies
      ORDER BY imdb_rating DESC, imdb_votes DESC LIMIT 20
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const sumNonRatedBudget = async db => {
  try {
    const { rows: movies } = await db.query(sql`
      SELECT SUM(production_budget)
      FROM movies
      WHERE mpaa_rating ILIKE 'not rated'
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const futureReleases = async db => {
  try {
    const { rows: movies } = await db.query(sql`
    SELECT title, release_date
    FROM movies
    WHERE DATE(release_date) > DATE(NOW())
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const releases50s80s = async db => {
  try {
    const { rows: movies } = await db.query(sql`
    SELECT title, us_gross, release_date
    FROM movies
    WHERE DATE(release_date) > '1950-01-01' AND DATE(release_date) < '1989-12-31'
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const zeroGross = async db => {
  try {
    const { rows: movies } = await db.query(sql`
    SELECT title, us_gross, worldwide_gross
    FROM movies
    WHERE us_gross = 0 OR worldwide_gross = 0
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const lowestUSgross = async db => {
  try {
    const { rows: movies } = await db.query(sql`
    SELECT title, us_gross
    FROM movies
    ORDER BY us_gross LIMIT 50
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const titleStartsWithF = async db => {
  try {
    const { rows: movies } = await db.query(sql`
    SELECT title, source
    FROM movies
    WHERE title ILIKE 'F%'
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const SonyPictures = async db => {
  try {
    const { rows: movies } = await db.query(sql`
    SELECT distributor, production_budget, creative_type, major_genre
    FROM movies
    WHERE production_budget > 10000000 AND distributor ILIKE 'Sony Pictures'
    `)
    return movies
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}



module.exports = {
  getAll,
  nonNull,
  lowBudget,
  mostExpensive,
  remakes,
  imdbRating,
  rottenTomatoes,
  imdbVotings,
  sumNonRatedBudget,
  futureReleases,
  releases50s80s,
  zeroGross,
  lowestUSgross,
  titleStartsWithF,
  SonyPictures,
  }