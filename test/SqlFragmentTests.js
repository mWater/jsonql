assert = require('chai').assert
SqlFragment = require '../src/SqlFragment'

describe "SqlFragment", ->
  it 'appends blank', ->
    fr = new SqlFragment("apple", [1]).append(new SqlFragment())
    assert.equal fr.sql, "apple"
    assert.deepEqual fr.params, [1]

  it 'appends other', ->
    fr = new SqlFragment("apple", [1]).append(new SqlFragment("banana", [2]))
    assert.equal fr.sql, "applebanana"
    assert.deepEqual fr.params, [1, 2]

  it 'converts number to inline', ->
    sql = new SqlFragment("x=?", [2]).toInline()
    assert.equal sql, "x=2"

  it 'converts string to inline', ->
    sql = new SqlFragment("x=?", ["abc"]).toInline()
    assert.equal sql, "x='abc'"

  it 'converts date to inline', ->
    date = new Date()

    sql = new SqlFragment("x=?", [date]).toInline()
    assert.equal sql, "x='" + date.toISOString() + "'"

  it 'escapes \' to inline', ->
    sql = new SqlFragment("x=?", ["a'bc"]).toInline()
    assert.equal sql, "x='a''bc'"

  it 'converts null to inline', ->
    sql = new SqlFragment("x=?", [null]).toInline()
    assert.equal sql, "x=null"

  it 'converts json to inline', ->
    sql = new SqlFragment("x=?", [{a:"'"}]).toInline()
    assert.equal sql, '''x=('{"a":"''"}'::json)'''

  it 'joins multiple with divider', ->
    fr = SqlFragment.join([new SqlFragment("a", [1]), new SqlFragment("b", [2])], " and ")
    assert.equal fr.sql, 'a and b'
    assert.deepEqual fr.params, [1, 2]
