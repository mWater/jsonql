gulp = require 'gulp'
gutil = require 'gulp-util'
coffee = require 'gulp-coffee' 
glob = require 'glob'
source = require 'vinyl-source-stream'
browserify = require 'browserify'
coffeeify = require 'coffeeify'

# Compile coffeescript to js in lib/
gulp.task 'coffee', ->
  gulp.src('./src/*.coffee')
    .pipe(coffee({ bare: true }))
    .pipe(gulp.dest('./lib/'))

gulp.task 'compile', gulp.series('coffee')

gulp.task 'default', gulp.series('compile')