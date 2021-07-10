const slonik = require('slonik')

const SLONIK_URL = require('../src/constants')
const db = slonik.createPool(SLONIK_URL)

module.exports = db