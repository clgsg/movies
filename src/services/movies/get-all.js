const { getAll } = require('../../queries/movies')

module.exports = db => async (req, res, next) => {
  const result = await getAll(db)

  if (result === false) {
    return next({ info: new Error("Couldn't retrieve movies") })
  }

  res.status(200).json({
    success: true,
    data: result,
  })
}