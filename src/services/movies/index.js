const router = require('express').Router()

module.exports = db => {
  router.get('/', require('./get-all')(db))
  return router
}