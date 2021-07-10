const router = require('express').Router()

module.exports = db => {
  router.use('/directors', require('./directors')(db))
  router.use('/movies', require('./movies')(db))
  router.use('/all', require('./directors-movies')(db))

  return router
}