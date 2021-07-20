import { assert } from 'chai';
import SqlFragment from '../src/SqlFragment';

describe("SqlFragment", function() {
  it('appends blank', function() {
    const fr = new SqlFragment("apple", [1]).append(new SqlFragment());
    assert.equal(fr.sql, "apple");
    return assert.deepEqual(fr.params, [1]);
});

  it('appends other', function() {
    const fr = new SqlFragment("apple", [1]).append(new SqlFragment("banana", [2]));
    assert.equal(fr.sql, "applebanana");
    return assert.deepEqual(fr.params, [1, 2]);
});

  it('converts number to inline', function() {
    const sql = new SqlFragment("x=?", [2]).toInline();
    return assert.equal(sql, "x=2");
  });

  it('converts string to inline', function() {
    const sql = new SqlFragment("x=?", ["abc"]).toInline();
    return assert.equal(sql, "x='abc'");
  });

  it('converts date to inline', function() {
    const date = new Date();

    const sql = new SqlFragment("x=?", [date]).toInline();
    return assert.equal(sql, "x='" + date.toISOString() + "'");
  });

  it('escapes \' to inline', function() {
    const sql = new SqlFragment("x=?", ["a'bc"]).toInline();
    return assert.equal(sql, "x='a''bc'");
  });

  it('converts null to inline', function() {
    const sql = new SqlFragment("x=?", [null]).toInline();
    return assert.equal(sql, "x=null");
  });

  it('converts json to inline', function() {
    const sql = new SqlFragment("x=?", [{a:"'"}]).toInline();
    return assert.equal(sql, 'x=(\'{"a":"\'\'"}\'::json)');
  });

  return it('joins multiple with divider', function() {
    const fr = SqlFragment.join([new SqlFragment("a", [1]), new SqlFragment("b", [2])], " and ");
    assert.equal(fr.sql, 'a and b');
    return assert.deepEqual(fr.params, [1, 2]);
});
});
