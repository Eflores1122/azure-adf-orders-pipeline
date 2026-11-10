/* Browser port of the local reject split in scripts/validate_adf.py.
   classify_order_row / explain_order_row decide good vs reject.
   File-name helpers follow the expressions in dataflow/df_orders_etl.json
   and are reported separately: filterDeliveryOrder is not part of the split. */
(function (root, factory) {
  var api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  } else {
    root.OrderSplit = api;
  }
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  var REASONS = ["quantity_ok", "quantity_missing", "quantity_blank", "quantity_not_numeric"];

  function pythonFloat(value) {
    if (value === null || value === undefined) return null;
    var text = String(value).trim();
    if (text === "") return null;
    if (/^[+-]?(?:nan|inf|infinity)$/i.test(text)) {
      if (/nan/i.test(text)) return NaN;
      return text.charAt(0) === "-" ? -Infinity : Infinity;
    }
    var stripped = "";
    for (var i = 0; i < text.length; i += 1) {
      if (text.charAt(i) === "_") {
        var prev = text.charAt(i - 1);
        var next = text.charAt(i + 1);
        if (!prev || !next || !/\d/.test(prev) || !/\d/.test(next)) return null;
        continue;
      }
      stripped += text.charAt(i);
    }
    if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(stripped)) return null;
    return Number(stripped);
  }

  function explainOrderRow(row) {
    if (!Object.prototype.hasOwnProperty.call(row, "Quantity") || row.Quantity === null || row.Quantity === undefined) {
      return ["reject", "quantity_missing"];
    }
    var text = String(row.Quantity).trim();
    if (text === "") return ["reject", "quantity_blank"];
    if (pythonFloat(text) === null) return ["reject", "quantity_not_numeric"];
    return ["good", "quantity_ok"];
  }

  function parseCsv(text) {
    var src = String(text);
    if (src.charCodeAt(0) === 0xfeff) src = src.slice(1);
    var rows = [];
    var row = [];
    var field = "";
    var inQuotes = false;
    var fieldStarted = false;
    var i = 0;

    function endRow() {
      if (!fieldStarted && row.length === 0) rows.push([]);
      else {
        row.push(field);
        rows.push(row);
      }
      row = [];
      field = "";
      fieldStarted = false;
    }

    while (i < src.length) {
      var ch = src.charAt(i);
      if (inQuotes) {
        if (ch === '"') {
          if (src.charAt(i + 1) === '"') {
            field += '"';
            fieldStarted = true;
            i += 2;
            continue;
          }
          inQuotes = false;
          fieldStarted = true;
          i += 1;
          continue;
        }
        field += ch;
        fieldStarted = true;
        i += 1;
        continue;
      }
      if (ch === '"') {
        inQuotes = true;
        fieldStarted = true;
        i += 1;
        continue;
      }
      if (ch === ",") {
        row.push(field);
        field = "";
        fieldStarted = true;
        i += 1;
        continue;
      }
      if (ch === "\r" || ch === "\n") {
        if (ch === "\r" && src.charAt(i + 1) === "\n") i += 1;
        endRow();
        i += 1;
        continue;
      }
      field += ch;
      fieldStarted = true;
      i += 1;
    }
    if (fieldStarted || row.length) {
      row.push(field);
      rows.push(row);
    }
    return rows;
  }

  function dictRows(matrix) {
    if (!matrix.length || matrix[0].length === 0) {
      throw new Error("CSV has no header");
    }
    var header = matrix[0].slice();
    var rows = [];
    for (var r = 1; r < matrix.length; r += 1) {
      var src = matrix[r];
      if (!src.length) continue;
      var obj = {};
      for (var c = 0; c < header.length; c += 1) {
        var name = header[c];
        if (!name) continue;
        obj[name] = c < src.length ? src[c] : null;
      }
      rows.push(obj);
    }
    return { header: header.filter(function (name) { return name; }), rows: rows };
  }

  function substringIndex(value, delim, count) {
    var text = String(value);
    if (count === 0 || delim === "") return "";
    var found;
    var index;
    if (count > 0) {
      var start = 0;
      for (index = 0; index < count; index += 1) {
        found = text.indexOf(delim, start);
        if (found < 0) return text;
        if (index === count - 1) return text.slice(0, found);
        start = found + delim.length;
      }
      return text;
    }
  var end = text.length;
  var remaining = -count;
  for (index = 0; index < remaining; index += 1) {
    found = text.lastIndexOf(delim, end - 1);
    if (found < 0) return text;
    if (index === remaining - 1) return text.slice(found + delim.length);
    end = found;
  }
  return text;
}

  function customerName(fileName) {
    return substringIndex(fileName, "_", 1);
  }

  function pipelineCustomer(fileName) {
    var chosen = fileName ? fileName : "BestLocalPizza_DeliveryOrder_20250828T053149Z.csv";
    return chosen.split("_")[0];
  }

  function emailTimestamp(fileName) {
    var stem = substringIndex(fileName, ".csv", 1);
    var raw = substringIndex(stem, "DeliveryOrder_", -1);
    var normalized = raw.replace(/T/g, " ").replace(/Z/g, "");
    var match = /^(\d{4})(\d{2})(\d{2}) (\d{2})(\d{2})(\d{2})$/.exec(normalized);
    if (!match) return null;
    var year = Number(match[1]);
    var month = Number(match[2]);
    var day = Number(match[3]);
    var hour = Number(match[4]);
    var minute = Number(match[5]);
    var second = Number(match[6]);
    var utc = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
    if (
      utc.getUTCFullYear() !== year ||
      utc.getUTCMonth() !== month - 1 ||
      utc.getUTCDate() !== day ||
      utc.getUTCHours() !== hour ||
      utc.getUTCMinutes() !== minute ||
      utc.getUTCSeconds() !== second
    ) {
      return null;
    }
    function pad(n) { return String(n).padStart(2, "0"); }
    return year + "-" + pad(month) + "-" + pad(day) + " " + pad(hour) + ":" + pad(minute) + ":" + pad(second);
  }

  function parseForDate(value) {
    if (value === null || value === undefined) return null;
    var text = String(value).trim();
    if (text === "") return null;
    var match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(text);
    if (!match) return null;
    var day = Number(match[1]);
    var month = Number(match[2]);
    var year = Number(match[3]);
    var utc = new Date(Date.UTC(year, month - 1, day));
    if (utc.getUTCFullYear() !== year || utc.getUTCMonth() !== month - 1 || utc.getUTCDate() !== day) return null;
    function pad(n) { return String(n).padStart(2, "0"); }
    return year + "-" + pad(month) + "-" + pad(day);
  }

  function fileGate(fileName, today) {
    var prefix = substringIndex(fileName, "_", 1) + "_DeliveryOrder_";
    var prefixOk = String(fileName).toUpperCase().indexOf(prefix.toUpperCase()) === 0;
    var last = substringIndex(fileName, "_", -1);
    var stamp = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(last);
    var stampDate = null;
    if (stamp) {
      var year = Number(stamp[1]);
      var month = Number(stamp[2]);
      var day = Number(stamp[3]);
      var hour = Number(stamp[4]);
      var minute = Number(stamp[5]);
      var second = Number(stamp[6]);
      var utc = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
      if (
        utc.getUTCFullYear() === year &&
        utc.getUTCMonth() === month - 1 &&
        utc.getUTCDate() === day &&
        utc.getUTCHours() === hour &&
        utc.getUTCMinutes() === minute &&
        utc.getUTCSeconds() === second
      ) {
        function pad(n) { return String(n).padStart(2, "0"); }
        stampDate = year + "-" + pad(month) + "-" + pad(day);
      }
    }
    var dateOk = stampDate !== null && stampDate === today;
    return {
      prefix: prefix,
      prefix_ok: prefixOk,
      last: last,
      stamp_date: stampDate,
      date_ok: dateOk,
      passes: prefixOk && dateOk
    };
  }

  function rejectOutPath(customer, fileName, date) {
    var d = date instanceof Date ? date : new Date(date);
    function pad(n) { return String(n).padStart(2, "0"); }
    var y = d.getUTCFullYear();
    var m = pad(d.getUTCMonth() + 1);
    var day = pad(d.getUTCDate());
    var hh = pad(d.getUTCHours());
    var mm = pad(d.getUTCMinutes());
    var ss = pad(d.getUTCSeconds());
    return y + "/" + m + "/" + day + "/" + customer + "/" + customer + "_rejects_" + y + m + day + hh + mm + ss + "_" + fileName;
  }

  function classifyText(fileName, text, today) {
    var table = dictRows(parseCsv(text));
    var rows = table.rows.map(function (values, index) {
      var explained = explainOrderRow(values);
      var forRaw = Object.prototype.hasOwnProperty.call(values, "ForDate") ? values.ForDate : null;
      return {
        line: index + 2,
        status: explained[0],
        reason: explained[1],
        values: values,
        for_date: parseForDate(forRaw),
        customer_name: customerName(fileName),
        email_timestamp: emailTimestamp(fileName),
        file_name: fileName
      };
    });
    var good = 0;
    var reject = 0;
    rows.forEach(function (row) {
      if (row.status === "good") good += 1;
      else reject += 1;
    });
    return {
      file_name: fileName,
      header: table.header,
      rows: rows,
      good: good,
      reject: reject,
      customer_name: customerName(fileName),
      pipeline_customer: pipelineCustomer(fileName),
      email_timestamp: emailTimestamp(fileName),
      gate: fileGate(fileName, today)
    };
  }

  function tokenize(sql) {
    var tokens = [];
    var i = 0;
    var keywords = {
      SELECT: 1, FROM: 1, WHERE: 1, GROUP: 1, BY: 1, ORDER: 1, ASC: 1, DESC: 1,
      LIMIT: 1, AND: 1, OR: 1, NOT: 1, IS: 1, NULL: 1, AS: 1, COUNT: 1, SUM: 1
    };
    while (i < sql.length) {
      var ch = sql.charAt(i);
      if (/\s/.test(ch)) { i += 1; continue; }
      if (ch === "-" && sql.charAt(i + 1) === "-") {
        while (i < sql.length && sql.charAt(i) !== "\n") i += 1;
        continue;
      }
      if (ch === "'" ) {
        var str = "";
        i += 1;
        while (i < sql.length) {
          if (sql.charAt(i) === "'" && sql.charAt(i + 1) === "'") {
            str += "'";
            i += 2;
            continue;
          }
          if (sql.charAt(i) === "'") { i += 1; break; }
          str += sql.charAt(i);
          i += 1;
        }
        tokens.push({ type: "string", value: str });
        continue;
      }
      if (ch === '"') {
        var ident = "";
        i += 1;
        while (i < sql.length && sql.charAt(i) !== '"') {
          ident += sql.charAt(i);
          i += 1;
        }
        if (sql.charAt(i) === '"') i += 1;
        tokens.push({ type: "ident", value: ident, quoted: true });
        continue;
      }
      if ("(),*=<>!".indexOf(ch) !== -1) {
        var two = sql.slice(i, i + 2);
        if (two === "<=" || two === ">=" || two === "!=" || two === "<>") {
          tokens.push({ type: "op", value: two === "<>" ? "!=" : two });
          i += 2;
          continue;
        }
        tokens.push({ type: ch === "*" ? "star" : (ch === "," || ch === "(" || ch === ")" ? ch : "op"), value: ch });
        i += 1;
        continue;
      }
      if (/[0-9.]/.test(ch)) {
        var num = "";
        while (i < sql.length && /[0-9.]/.test(sql.charAt(i))) {
          num += sql.charAt(i);
          i += 1;
        }
        if (!/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(num)) throw new Error("SQL: bad number " + num);
        tokens.push({ type: "number", value: Number(num) });
        continue;
      }
      if (/[A-Za-z_]/.test(ch)) {
        var word = "";
        while (i < sql.length && /[A-Za-z0-9_]/.test(sql.charAt(i))) {
          word += sql.charAt(i);
          i += 1;
        }
        if (keywords[word.toUpperCase()]) tokens.push({ type: "kw", value: word.toUpperCase() });
        else tokens.push({ type: "ident", value: word, quoted: false });
        continue;
      }
      throw new Error("SQL: unexpected character " + ch);
    }
    tokens.push({ type: "eof" });
    return tokens;
  }

  function Parser(tokens) {
    this.tokens = tokens;
    this.i = 0;
  }

  Parser.prototype.peek = function () {
    return this.tokens[this.i];
  };

  Parser.prototype.eat = function (type, value) {
    var token = this.peek();
    if (token.type !== type) return null;
    if (value !== undefined && token.value !== value) return null;
    this.i += 1;
    return token;
  };

  Parser.prototype.expect = function (type, value) {
    var token = this.eat(type, value);
    if (!token) {
      var got = this.peek();
      throw new Error("SQL: expected " + (value || type) + " near " + (got.value || got.type));
    }
    return token;
  };

  function parseQuery(parser) {
    parser.expect("kw", "SELECT");
    var select = [];
    if (parser.eat("star")) {
      select.push({ star: true, alias: "*" });
    } else {
      select.push(parseSelectItem(parser));
      while (parser.eat(",")) {
        if (parser.eat("star")) select.push({ star: true, alias: "*" });
        else select.push(parseSelectItem(parser));
      }
    }
    parser.expect("kw", "FROM");
    var tableToken = parser.expect("ident");
    var table = tableToken.value.toLowerCase();
    if (table !== "run" && table !== "accepted" && table !== "rejected") {
      throw new Error("SQL: table must be run, accepted, or rejected");
    }
    var where = null;
    if (parser.eat("kw", "WHERE")) where = parseOr(parser);
    var groupBy = null;
    if (parser.eat("kw", "GROUP")) {
      parser.expect("kw", "BY");
      groupBy = [parseValue(parser)];
      while (parser.eat(",")) groupBy.push(parseValue(parser));
    }
    var orderBy = null;
    if (parser.eat("kw", "ORDER")) {
      parser.expect("kw", "BY");
      orderBy = [parseOrderItem(parser)];
      while (parser.eat(",")) orderBy.push(parseOrderItem(parser));
    }
    var limit = null;
    if (parser.eat("kw", "LIMIT")) {
      var limitToken = parser.expect("number");
      if (!Number.isInteger(limitToken.value) || limitToken.value < 0) throw new Error("SQL: LIMIT must be a non-negative integer");
      limit = limitToken.value;
    }
    if (parser.peek().type !== "eof") throw new Error("SQL: unexpected " + (parser.peek().value || parser.peek().type));
    return { select: select, table: table, where: where, groupBy: groupBy, orderBy: orderBy, limit: limit };
  }

  function parseSelectItem(parser) {
    var expr = parseValue(parser);
    var alias = expr.alias;
    if (parser.eat("kw", "AS")) {
      var aliasToken = parser.eat("ident") || parser.eat("kw");
      if (!aliasToken) throw new Error("SQL: expected alias");
      alias = aliasToken.value;
    }
    expr.alias = alias;
    return expr;
  }

  function parseOrderItem(parser) {
    var expr = parseValue(parser);
    var dir = "ASC";
    if (parser.eat("kw", "ASC")) dir = "ASC";
    else if (parser.eat("kw", "DESC")) dir = "DESC";
    return { expr: expr, dir: dir };
  }

  function parseValue(parser) {
    if (parser.eat("kw", "COUNT")) {
      parser.expect("(");
      if (parser.eat("star")) {
        parser.expect(")");
        return { kind: "agg", fn: "count_star", alias: "COUNT(*)" };
      }
      var arg = parseIdentExpr(parser);
      parser.expect(")");
      return { kind: "agg", fn: "count", arg: arg, alias: "COUNT(" + arg.name + ")" };
    }
    if (parser.eat("kw", "SUM")) {
      parser.expect("(");
      var sumArg = parseIdentExpr(parser);
      parser.expect(")");
      return { kind: "agg", fn: "sum", arg: sumArg, alias: "SUM(" + sumArg.name + ")" };
    }
    if (parser.peek().type === "number") {
      var number = parser.expect("number");
      return { kind: "number", value: number.value, alias: String(number.value) };
    }
    if (parser.peek().type === "op" && parser.peek().value === "-") {
      parser.expect("op", "-");
      var negated = parser.expect("number");
      return { kind: "number", value: -negated.value, alias: String(-negated.value) };
    }
    if (parser.peek().type === "string") {
      var str = parser.expect("string");
      return { kind: "string", value: str.value, alias: "'" + str.value + "'" };
    }
    return parseIdentExpr(parser);
  }

  function parseIdentExpr(parser) {
    var ident = parser.expect("ident");
    return { kind: "col", name: ident.value, quoted: !!ident.quoted, alias: ident.value };
  }

  function parseOr(parser) {
    var left = parseAnd(parser);
    while (parser.eat("kw", "OR")) {
      left = { kind: "or", left: left, right: parseAnd(parser) };
    }
    return left;
  }

  function parseAnd(parser) {
    var left = parseNot(parser);
    while (parser.eat("kw", "AND")) {
      left = { kind: "and", left: left, right: parseNot(parser) };
    }
    return left;
  }

  function parseNot(parser) {
    if (parser.eat("kw", "NOT")) return { kind: "not", expr: parseNot(parser) };
    return parsePredicate(parser);
  }

  function parsePredicate(parser) {
    if (parser.eat("(")) {
      var inner = parseOr(parser);
      parser.expect(")");
      return inner;
    }
    var left = parseValue(parser);
    if (parser.eat("kw", "IS")) {
      var negated = !!parser.eat("kw", "NOT");
      parser.expect("kw", "NULL");
      return { kind: "isnull", expr: left, negated: negated };
    }
    var op = parser.expect("op");
    var right = parseValue(parser);
    return { kind: "cmp", op: op.value, left: left, right: right };
  }

  var COMPUTED = ["line", "status", "reason", "file_name", "customer_name", "for_date", "email_timestamp"];

  function lookup(row, name, quoted) {
    if (!quoted) {
      var key = name.toLowerCase();
      if (key === "line") return row.line;
      if (key === "status") return row.status;
      if (key === "reason") return row.reason;
      if (key === "file_name") return row.file_name;
      if (key === "customer_name") return row.customer_name;
      if (key === "for_date") return row.for_date;
      if (key === "email_timestamp") return row.email_timestamp;
      var found = false;
      var value = null;
      Object.keys(row.values).forEach(function (header) {
        if (header.toLowerCase() === key) {
          found = true;
          value = row.values[header];
        }
      });
      if (found) return value;
    } else if (Object.prototype.hasOwnProperty.call(row.values, name)) {
      return row.values[name];
    }
    throw new Error("SQL: unknown column " + name);
  }

  function evalExpr(expr, row) {
    if (expr.kind === "number") return expr.value;
    if (expr.kind === "string") return expr.value;
    if (expr.kind === "col") return lookup(row, expr.name, expr.quoted);
    throw new Error("SQL: aggregate used where a value is required");
  }

  function asComparableNumber(value) {
    if (typeof value === "number") return Number.isNaN(value) ? null : value;
    if (value === null || value === undefined) return null;
    var parsed = pythonFloat(String(value));
    if (parsed === null || Number.isNaN(parsed)) return null;
    return parsed;
  }

  function compareValues(left, right, op) {
    if (left === null || left === undefined || right === null || right === undefined) return false;
    var ln = asComparableNumber(left);
    var rn = asComparableNumber(right);
    var a;
    var b;
    var numeric = ln !== null && rn !== null;
    if (numeric) {
      a = ln;
      b = rn;
    } else {
      a = String(left);
      b = String(right);
    }
    if (op === "=") return a === b;
    if (op === "!=") return a !== b;
    if (op === "<") return a < b;
    if (op === ">") return a > b;
    if (op === "<=") return a <= b;
    if (op === ">=") return a >= b;
    throw new Error("SQL: unknown operator " + op);
  }

  function evalPredicate(node, row) {
    if (node.kind === "or") return evalPredicate(node.left, row) || evalPredicate(node.right, row);
    if (node.kind === "and") return evalPredicate(node.left, row) && evalPredicate(node.right, row);
    if (node.kind === "not") return !evalPredicate(node.expr, row);
    if (node.kind === "isnull") {
      var value = evalExpr(node.expr, row);
      var missing = value === null || value === undefined;
      return node.negated ? !missing : missing;
    }
    if (node.kind === "cmp") return compareValues(evalExpr(node.left, row), evalExpr(node.right, row), node.op);
    throw new Error("SQL: bad predicate");
  }

  function exprKey(expr) {
    if (expr.kind === "col") return "col:" + (expr.quoted ? JSON.stringify(expr.name) : expr.name.toLowerCase());
    if (expr.kind === "number") return "num:" + expr.value;
    if (expr.kind === "string") return "str:" + expr.value;
    return "other";
  }

  function columnList(rows) {
    var header = [];
    var seen = {};
    function add(name) {
      if (seen[name]) return;
      seen[name] = true;
      header.push(name);
    }
    COMPUTED.forEach(add);
    if (rows.length) Object.keys(rows[0].values).forEach(add);
    return header;
  }

  function expandSelect(select, rows) {
    var expanded = [];
    select.forEach(function (item) {
      if (!item.star) {
        expanded.push(item);
        return;
      }
      columnList(rows).forEach(function (name) {
        expanded.push({ kind: "col", name: name, quoted: false, alias: name });
      });
    });
    return expanded;
  }

  function aggregate(item, group) {
    if (item.fn === "count_star") return group.length;
    if (item.fn === "count") {
      var n = 0;
      group.forEach(function (row) {
        var value = evalExpr(item.arg, row);
        if (value !== null && value !== undefined) n += 1;
      });
      return n;
    }
    if (item.fn === "sum") {
      var sum = null;
      group.forEach(function (row) {
        var value = evalExpr(item.arg, row);
        var num = asComparableNumber(value);
        if (num === null || !Number.isFinite(num)) return;
        sum = (sum === null ? 0 : sum) + num;
      });
      return sum;
    }
    throw new Error("SQL: unknown aggregate");
  }

  function project(select, group) {
    var sample = group[0] || null;
    var out = {};
    var columns = [];
    select.forEach(function (item) {
      var value;
      if (item.kind === "agg") value = aggregate(item, group);
      else if (!sample) value = null;
      else value = evalExpr(item, sample);
      var alias = item.alias;
      var unique = alias;
      var salt = 2;
      while (Object.prototype.hasOwnProperty.call(out, unique)) {
        unique = alias + "_" + salt;
        salt += 1;
      }
      out[unique] = value;
      columns.push(unique);
    });
    return { columns: columns, values: out };
  }

  function isAggregateQuery(select) {
    return select.some(function (item) { return item.kind === "agg"; });
  }

  function runSql(sql, rows) {
    var parser = new Parser(tokenize(String(sql).trim().replace(/;+\s*$/, "")));
    var query = parseQuery(parser);
    var data = rows.slice();
    if (query.table === "accepted") data = data.filter(function (row) { return row.status === "good"; });
    if (query.table === "rejected") data = data.filter(function (row) { return row.status === "reject"; });
    if (query.where) data = data.filter(function (row) { return evalPredicate(query.where, row); });
    var select = expandSelect(query.select, rows);
    var grouped = isAggregateQuery(select) || query.groupBy;
    var projected = [];
    if (!grouped) {
      projected = data.map(function (row) { return project(select, [row]); });
    } else if (!query.groupBy) {
      projected = [project(select, data)];
    } else {
      if (isAggregateQuery(select)) {
        select.forEach(function (item) {
          if (item.kind === "agg" || item.star) return;
          var key = exprKey(item);
          var ok = query.groupBy.some(function (groupExpr) { return exprKey(groupExpr) === key; });
          if (!ok) throw new Error("SQL: " + item.alias + " is not in GROUP BY");
        });
      }
      var groups = new Map();
      data.forEach(function (row) {
        var key = JSON.stringify(query.groupBy.map(function (expr) { return evalExpr(expr, row); }));
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(row);
      });
      groups.forEach(function (group) { projected.push(project(select, group)); });
    }
    if (query.orderBy) {
      projected.sort(function (a, b) {
        for (var i = 0; i < query.orderBy.length; i += 1) {
          var item = query.orderBy[i];
          var name = item.expr.alias;
          var av = Object.prototype.hasOwnProperty.call(a.values, name) ? a.values[name] : evalLoose(item.expr, a);
          var bv = Object.prototype.hasOwnProperty.call(b.values, name) ? b.values[name] : evalLoose(item.expr, b);
          if (av === bv) continue;
          if (av === null || av === undefined) return 1;
          if (bv === null || bv === undefined) return -1;
          var an = asComparableNumber(av);
          var bn = asComparableNumber(bv);
          var delta = an !== null && bn !== null ? (an < bn ? -1 : 1) : (String(av) < String(bv) ? -1 : 1);
          if (item.dir === "DESC") delta = -delta;
          if (delta) return delta;
        }
        return 0;
      });
    }
    if (query.limit !== null) projected = projected.slice(0, query.limit);
    var columns = projected.length ? projected[0].columns : select.map(function (item) { return item.alias; });
    return {
      columns: columns,
      rows: projected.map(function (item) { return item.values; })
    };
  }

  function evalLoose(expr, projected) {
    if (expr.kind === "col" && Object.prototype.hasOwnProperty.call(projected.values, expr.name)) return projected.values[expr.name];
    if (expr.kind === "number") return expr.value;
    if (expr.kind === "string") return expr.value;
    return null;
  }

  return {
    REASONS: REASONS,
    pythonFloat: pythonFloat,
    explainOrderRow: explainOrderRow,
    parseCsv: parseCsv,
    dictRows: dictRows,
    substringIndex: substringIndex,
    customerName: customerName,
    pipelineCustomer: pipelineCustomer,
    emailTimestamp: emailTimestamp,
    parseForDate: parseForDate,
    fileGate: fileGate,
    rejectOutPath: rejectOutPath,
    classifyText: classifyText,
    runSql: runSql
  };
});
