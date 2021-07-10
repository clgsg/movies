const { sql } = require('slonik')

const getAll = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT * FROM directors
    `)

    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const nonEmptyNames = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT name 
      FROM directors
      WHERE name NOT NULL
    `)
    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const qNameNicknames = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT query_name, nickname 
      FROM directors
    `)
    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const picNicknames = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT pic, nickname 
      FROM directors
      WHERE nickname NOT NULL
    `)
    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const qNameCanadian = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT query_name, nationality 
      FROM directors
      WHERE nationality LIKE 'canadiense'
    `)
    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const qNameBRUS = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT query_name, nationality 
      FROM directors
      WHERE nationality ~ 'brit' AND nationality ~ 'estadoun'
    `)
    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}


const chess = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT query_name, nationality, roles
      FROM directors
      WHERE roles LIKE 'ajedrecista'
    `)
    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const dualNationality = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT query_name, name, nationality
      FROM directors
      WHERE nationality ~ ','
    `)
    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

const severalRoles = async db => {
  try {
    const { rows: directors } = await db.query(sql`
      SELECT query_name, roles
      FROM directors
      WHERE roles LIKE '%,%,%'
    `)
    return directors
  } catch (error) {
    console.info('> error: ', error.message)
    return false
  }
}

module.exports = {
  getAll,
  nonEmptyNames,
  qNameNicknames,
  picNicknames,
  qNameCanadian,
  qNameBRUS,
  chess,
  dualNationality,
  severalRoles,
}