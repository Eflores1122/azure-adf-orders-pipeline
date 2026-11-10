(function () {
  "use strict";

  var STR = {
    es: {
      title: "pl_ordenes, pedidos CSV",
      langButton: "English",
      skip: "Saltar a las filas",
      intro: "La página lee el CSV y aplica classify_order_row. Una fila es válida cuando Quantity existe y float() la convierte. Si la columna falta, va vacía o no es numérica, la fila es rechazo.",
      sampleLabel: "CSV de ejemplo",
      run: "Correr el pipeline con datos de ejemplo",
      uploadLabel: "Subir un CSV",
      loading: "Leyendo los CSV de samples/.",
      show: "Mostrar",
      all: "Todas",
      good: "Válidas",
      reject: "Rechazos",
      validWord: "Válida",
      rejectWord: "Rechazo",
      line: "Fila",
      status: "Estado",
      reason: "Motivo",
      forDateSql: "ForDate SQL",
      runTitle: "Esta corrida",
      dictLine: "BusquedaCosto no corre aquí. dict/Dict_USD.csv no está en el repositorio y el origen Diccionario tiene ignoreNoFilesFound: false, así que un flujo publicado se detiene si el archivo falta.",
      pipeTitle: "Diagrama de pl_ordenes",
      pipeNote: "Actividades y dependencias Completed de pipeline/pl_ordenes.json. El correo y la Logic App de ingesta no están en el repositorio.",
      dBlob: ".csv bajo /raw/blobs/",
      dTrg: "pasa p_folderPath y p_fileName",
      dPl: "factoría adf-orders-pipeline, mexicocentral",
      dDf: "df_orders_etl, General, 8 núcleos",
      flowTitle: "Dentro de df_orders_etl",
      sOrders: "ds_orders_raw_csv, RawPath, ruta comodín fija",
      sDict: "ds_dict_usd_csv, dict/Dict_USD.csv",
      sDerive: "CustomerName, EmailTimestamp, Quantity, ForDate",
      sFilter: "prefijo y patrón de fecha",
      sSort: "sort ascendente",
      goodSink: "Insert en ds_sql_orders, tabla dbo.Orders. El número de esta corrida es el split de Quantity, no una inserción real.",
      badSink: "DerivarRechazos escribe RejectReason = 'Quantity is NULL' y OutFileName. sinkRejects usa ds_rejects_csv.",
      chartTitle: "Conteo medido",
      lineTitle: "Linaje de la fila",
      colStep: "Paso",
      colExpr: "Expresión en df_orders_etl.json",
      colHere: "En esta página",
      lFile: "FileName es el nombre del archivo elegido o subido.",
      lCustomer: "Se calcula en cada fila. set_customer usa el primer segmento de split por _.",
      lEmail: "Se calcula. Queda nulo cuando el nombre no trae ese timestamp.",
      lQty: "El veredicto es float() de classify_order_row. 10.5 es válida. Esta página no llama toInteger.",
      lDate: "Se parsea. Una fecha inválida no rechaza la fila.",
      lGate: "Se informa con la fecha UTC de hoy. No cambia el conteo de válidas y rechazos.",
      lLookup: "Sin diccionario en el repositorio. IDProduct y Sale Cost (USD) no se rellenan.",
      lSplit: "Válida o rechazo, con el motivo de explain_order_row.",
      lReason: "Ese texto corresponde a Quantity ausente o vacía. Una Quantity no numérica se rechaza por float() y el JSON no escribe otro motivo.",
      lSink: "Las dos ramas de la tabla. No hay base de datos en esta página.",
      lAlert: "El cuerpo usa el conteo medido, v_customerName y los marcadores de la canalización.",
      sqlTitle: "Consulta sobre esta corrida",
      sqlNote: "Motor de esta página, no Azure SQL. Tablas: run, accepted, rejected. Acepta SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, COUNT y SUM. Los números salen de las filas ya clasificadas.",
      sqlLabel: "SQL",
      runSql: "Ejecutar SQL",
      mermaidTitle: "Mermaid del repositorio",
      mermaidNote: "El dibujo de arriba usa los mismos nodos que Diagrams.md. El fuente Mermaid queda aquí.",
      mermaidSummary: "Arquitectura y flujo de datos",
      typeNote: "Tipografía Atkinson Hyperlegible, del Braille Institute, licencia OFL. En estas tablas se leen identificadores y cantidades, y esa fuente separa 0 de O y 1 de I. El verde #0e5c4a marca solo las filas y los conteos que classify_order_row deja como válidos.",
      source: "Fuente: classify_order_row en scripts/validate_adf.py.",
      uploaded: "archivo subido",
      readError: "No se pudo leer los CSV de ejemplo.",
      csvError: "El CSV no se pudo clasificar.",
      noHeader: "El CSV no tiene encabezado.",
      mismatch: "El navegador no coincide con explain_csv de scripts/validate_adf.py.",
      showing: "Mostrando las primeras 500 filas de",
      nullWord: "nulo",
      yes: "sí",
      no: "no",
      reasons: {
        quantity_ok: "Quantity numérica",
        quantity_missing: "Falta la columna Quantity",
        quantity_blank: "Quantity vacía",
        quantity_not_numeric: "Quantity no numérica"
      }
    },
    en: {
      title: "pl_ordenes, order CSV",
      langButton: "Español",
      skip: "Skip to rows",
      intro: "This page reads the CSV and applies classify_order_row. A row is valid when Quantity exists and float() can convert it. A missing column, a blank value, or a non-numeric value is a reject.",
      sampleLabel: "Sample CSV",
      run: "Run the pipeline on the sample data",
      uploadLabel: "Upload a CSV",
      loading: "Reading the CSVs in samples/.",
      show: "Show",
      all: "All",
      good: "Valid",
      reject: "Rejects",
      validWord: "Valid",
      rejectWord: "Reject",
      line: "Row",
      status: "Status",
      reason: "Reason",
      forDateSql: "ForDate SQL",
      runTitle: "This run",
      dictLine: "BusquedaCosto does not run here. dict/Dict_USD.csv is not in the repository, and the Diccionario source sets ignoreNoFilesFound: false, so a published flow stops when that file is missing.",
      pipeTitle: "pl_ordenes diagram",
      pipeNote: "Activities and Completed dependencies from pipeline/pl_ordenes.json. Mail and the ingest Logic App are not in this repository.",
      dBlob: ".csv under /raw/blobs/",
      dTrg: "passes p_folderPath and p_fileName",
      dPl: "factory adf-orders-pipeline, mexicocentral",
      dDf: "df_orders_etl, General, 8 cores",
      flowTitle: "Inside df_orders_etl",
      sOrders: "ds_orders_raw_csv, RawPath, fixed wildcard path",
      sDict: "ds_dict_usd_csv, dict/Dict_USD.csv",
      sDerive: "CustomerName, EmailTimestamp, Quantity, ForDate",
      sFilter: "prefix and date pattern",
      sSort: "ascending sort",
      goodSink: "Insert into ds_sql_orders, table dbo.Orders. The count for this run is the Quantity split, not a real insert.",
      badSink: "DerivarRechazos writes RejectReason = 'Quantity is NULL' and OutFileName. sinkRejects uses ds_rejects_csv.",
      chartTitle: "Measured counts",
      lineTitle: "Row lineage",
      colStep: "Step",
      colExpr: "Expression in df_orders_etl.json",
      colHere: "On this page",
      lFile: "FileName is the chosen or uploaded file name.",
      lCustomer: "Computed on every row. set_customer uses the first segment of a split on _.",
      lEmail: "Computed. Null when the name has no timestamp the pattern can read.",
      lQty: "The verdict is float() from classify_order_row. 10.5 is valid. This page does not call toInteger.",
      lDate: "Parsed. An invalid date does not reject the row.",
      lGate: "Reported against today's UTC date. It does not change the valid and reject counts.",
      lLookup: "No dictionary file is in the repository. IDProduct and Sale Cost (USD) stay empty.",
      lSplit: "Valid or reject, with the explain_order_row reason.",
      lReason: "That string covers a missing or blank Quantity. A non-numeric Quantity is rejected by float(), and the JSON does not write another reason.",
      lSink: "The two branches of the table. This page has no database.",
      lAlert: "The body uses the measured count, v_customerName, and the pipeline placeholders.",
      sqlTitle: "Query this run",
      sqlNote: "Engine in this page, not Azure SQL. Tables: run, accepted, rejected. It accepts SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, COUNT, and SUM. Figures come from the rows already classified.",
      sqlLabel: "SQL",
      runSql: "Run SQL",
      mermaidTitle: "Mermaid from the repository",
      mermaidNote: "The drawing above uses the same nodes as Diagrams.md. The Mermaid source is here.",
      mermaidSummary: "Architecture and data flow",
      typeNote: "Typeface: Atkinson Hyperlegible, Braille Institute, OFL. These tables mix identifiers and quantities, and that face keeps 0 apart from O and 1 apart from I. Green #0e5c4a marks only the rows and counts classify_order_row accepts.",
      source: "Source: classify_order_row in scripts/validate_adf.py.",
      uploaded: "uploaded file",
      readError: "The sample CSVs could not be read.",
      csvError: "The CSV could not be classified.",
      noHeader: "The CSV has no header.",
      mismatch: "The browser does not match explain_csv in scripts/validate_adf.py.",
      showing: "Showing the first 500 rows of",
      nullWord: "null",
      yes: "yes",
      no: "no",
      reasons: {
        quantity_ok: "Quantity is numeric",
        quantity_missing: "Quantity column is missing",
        quantity_blank: "Quantity is blank",
        quantity_not_numeric: "Quantity is not numeric"
      }
    }
  };

  var state = {
    lang: "es",
    samples: [],
    current: null,
    filter: "all"
  };

  function t(key) {
    return STR[state.lang][key];
  }

  function pair(good, reject) {
    if (state.lang === "es") {
      return count(good, "válida", "válidas") + ", " + count(reject, "rechazo", "rechazos");
    }
    return count(good, "valid", "valid") + ", " + count(reject, "reject", "rejects");
  }

  function count(n, one, many) {
    return n + " " + (n === 1 ? one : many);
  }

  function utcToday() {
    return new Date().toISOString().slice(0, 10);
  }

  function applyStatic() {
    document.documentElement.lang = state.lang;
    document.title = t("title");
    document.querySelectorAll("[data-i18n]").forEach(function (el) {
      var key = el.getAttribute("data-i18n");
      var value = STR[state.lang][key];
      if (typeof value === "string") el.textContent = value;
    });
  }

  function reasonText(code) {
    return STR[state.lang].reasons[code] || code;
  }

  function setText(id, text) {
    var el = document.getElementById(id);
    el.textContent = text;
    return el;
  }

  function renderHeadline(run) {
    document.getElementById("headline").textContent = pair(run.good, run.reject);
  }

  function lede(run, entry) {
    var parts = [run.file_name];
    if (entry && entry.uploaded) parts.push("(" + t("uploaded") + ")");
    var rejects = run.rows.filter(function (row) { return row.status === "reject"; });
    if (rejects.length && rejects.length <= 3) {
      parts.push(rejects.map(function (row) {
        return (state.lang === "es" ? "Fila " : "Row ") + row.line + ": " + reasonText(row.reason);
      }).join(". "));
    }
    parts.push(t("source"));
    return parts.join(". ").replace("..", ".");
  }

  function visibleRows(run) {
    return run.rows.filter(function (row) {
      if (state.filter === "good") return row.status === "good";
      if (state.filter === "reject") return row.status === "reject";
      return true;
    });
  }

  function renderTable(run) {
    var head = document.getElementById("thead");
    var body = document.getElementById("tbody");
    head.replaceChildren();
    body.replaceChildren();
    var columns = [t("line"), t("status"), t("reason")].concat(run.header).concat([t("forDateSql"), "CustomerName", "EmailTimestamp"]);
    var tr = document.createElement("tr");
    columns.forEach(function (name) {
      var th = document.createElement("th");
      th.textContent = name;
      tr.appendChild(th);
    });
    head.appendChild(tr);
    var rows = visibleRows(run);
    var clipped = rows.length > 500;
    rows.slice(0, 500).forEach(function (row) {
      var line = document.createElement("tr");
      addCell(line, String(row.line));
      var status = document.createElement("td");
      status.textContent = row.status === "good" ? t("validWord") : t("rejectWord");
      status.className = row.status === "good" ? "good" : "reject";
      line.appendChild(status);
      addCell(line, reasonText(row.reason));
      run.header.forEach(function (name) {
        var value = row.values[name];
        addCell(line, value === null || value === undefined ? "" : String(value));
      });
      addCell(line, row.for_date || t("nullWord"), !row.for_date);
      addCell(line, row.customer_name || "");
      addCell(line, row.email_timestamp || t("nullWord"), !row.email_timestamp);
      body.appendChild(line);
    });
    var caption = run.file_name + ". " + pair(run.good, run.reject) + ".";
    if (clipped) caption += " " + t("showing") + " " + rows.length + ".";
    document.getElementById("caption").textContent = caption;
  }

  function addCell(tr, text, muted) {
    var td = document.createElement("td");
    td.textContent = text;
    if (muted) td.className = "null";
    tr.appendChild(td);
  }

  function renderGate(run) {
    var gate = run.gate;
    var prefix = (state.lang === "es" ? "Prefijo " : "Prefix ") + (gate.prefix_ok ? t("yes") : t("no")) + " (" + gate.prefix + ").";
    var date = (state.lang === "es" ? " Fecha del nombre " : " Name date ") + (gate.date_ok ? t("yes") : t("no")) + ".";
    var last = (state.lang === "es" ? " Último segmento: " : " Last segment: ") + gate.last + ".";
    var today = (state.lang === "es" ? " UTC de hoy: " : " UTC today: ") + utcToday() + ".";
    var effect = state.lang === "es"
      ? " filterDeliveryOrder no cambia este conteo."
      : " filterDeliveryOrder does not change this count.";
    if (!gate.passes) {
      effect += state.lang === "es"
        ? " Con este nombre el flujo versionado no dejaría llegar las filas a SelectionRows."
        : " With this name the checked-in flow would not let the rows reach SelectionRows.";
    }
    setText("gate", prefix + date + last + today + effect);
    var email = run.email_timestamp
      ? "EmailTimestamp " + run.email_timestamp + "."
      : (state.lang === "es"
        ? "EmailTimestamp nulo. El nombre no trae un timestamp que el patrón yyyyMMdd HHmmss pueda leer."
        : "EmailTimestamp is null. The name has no timestamp the yyyyMMdd HHmmss pattern can read.");
    setText("email-line", email);
  }

  function renderAlert(run) {
    var customer = document.getElementById("d-customer");
    customer.textContent = "v_customerName = " + run.pipeline_customer;
    var step = document.getElementById("alert-step");
    var meta = document.getElementById("d-alert");
    var payload = document.getElementById("payload");
    var body = {
      fileName: run.file_name,
      customerName: run.pipeline_customer,
      rejects: run.reject,
      blobUrl: "https://<storage-account-name>.blob.core.windows.net/raw/<p_folderPath>/" + run.file_name
    };
    if (run.reject > 0) {
      step.classList.add("is-on");
      meta.textContent = state.lang === "es" ? "web_alerta haría POST" : "web_alerta would POST";
      setText("alert", state.lang === "es"
        ? "if_hay_rechazos es verdadero porque el conteo de rechazos es " + run.reject + ". p_folderPath no viene en el CSV, así que blobUrl conserva el marcador."
        : "if_hay_rechazos is true because the reject count is " + run.reject + ". p_folderPath is not in the CSV, so blobUrl keeps the placeholder.");
      payload.hidden = false;
      payload.textContent = JSON.stringify(body, null, 2);
      var now = new Date();
      setText("outpath", "OutFileName " + OrderSplit.rejectOutPath(run.customer_name, run.file_name, now));
    } else {
      step.classList.remove("is-on");
      meta.textContent = state.lang === "es" ? "web_alerta no corre" : "web_alerta does not run";
      setText("alert", state.lang === "es"
        ? "if_hay_rechazos es falso. sinkRejects.rowsWritten sería 0 y web_alerta no hace POST."
        : "if_hay_rechazos is false. sinkRejects.rowsWritten would be 0 and web_alerta does not POST.");
      payload.hidden = true;
      payload.textContent = "";
      setText("outpath", "");
    }
    setText("good-count", state.lang === "es"
      ? count(run.good, "válida", "válidas")
      : count(run.good, "valid", "valid"));
    setText("bad-count", state.lang === "es"
      ? count(run.reject, "rechazo", "rechazos")
      : count(run.reject, "reject", "rejects"));
  }

  function renderWildcard() {
    var name = "BestLocalPizza_DeliveryOrder_20250828T053149Z.csv";
    var gate = OrderSplit.fileGate(name, utcToday());
    var text = state.lang === "es"
      ? "Ruta comodín de Pedidos: " + name + ". Prefijo " + (gate.prefix_ok ? "sí" : "no") + ". Último segmento " + gate.last + ". El patrón yyyyMMdd'T'HHmmss'Z' no coincide porque el segmento conserva .csv."
      : "Pedidos wildcard path: " + name + ". Prefix " + (gate.prefix_ok ? "yes" : "no") + ". Last segment " + gate.last + ". The pattern yyyyMMdd'T'HHmmss'Z' does not match because the segment still includes .csv.";
    setText("wildcard", text);
  }

  function renderBars() {
    var host = document.getElementById("bars");
    host.replaceChildren();
    var items = state.samples.map(function (entry) {
      return { name: entry.name, good: entry.run.good, reject: entry.run.reject, current: state.current && !state.current.uploaded && state.current.name === entry.name };
    });
    if (state.current && state.current.uploaded) {
      items.push({
        name: state.current.name,
        good: state.current.run.good,
        reject: state.current.run.reject,
        current: true
      });
    }
    var max = 1;
    var goodTotal = 0;
    var rejectTotal = 0;
    items.forEach(function (item) {
      max = Math.max(max, item.good + item.reject);
      if (!state.current || !state.current.uploaded || item.name !== state.current.name || items.indexOf(item) < state.samples.length) {
        /* totals for the four samples only */
      }
    });
    state.samples.forEach(function (entry) {
      goodTotal += entry.run.good;
      rejectTotal += entry.run.reject;
    });
    items.forEach(function (item) {
      var row = document.createElement("div");
      row.className = "bar-row";
      var name = document.createElement("span");
      name.className = "bar-name" + (item.current ? " is-current" : "");
      name.textContent = item.name;
      var track = document.createElement("span");
      track.className = "bar-track";
      track.setAttribute("aria-hidden", "true");
      if (item.good) {
        var good = document.createElement("span");
        good.className = "bar-good";
        good.style.width = ((item.good / max) * 100) + "%";
        track.appendChild(good);
      }
      if (item.reject) {
        var bad = document.createElement("span");
        bad.className = "bar-bad";
        bad.style.width = ((item.reject / max) * 100) + "%";
        track.appendChild(bad);
      }
      var label = document.createElement("span");
      label.textContent = pair(item.good, item.reject);
      row.appendChild(name);
      row.appendChild(track);
      row.appendChild(label);
      host.appendChild(row);
    });
    var note = state.lang === "es"
      ? "Medido en esta página con classify_order_row sobre samples/. " + pair(goodTotal, rejectTotal) + " en " + state.samples.length + " archivos. La barra verde es válida. El trazo es rechazo."
      : "Measured on this page with classify_order_row over samples/. " + pair(goodTotal, rejectTotal) + " across " + state.samples.length + " files. The green bar is valid. The outline is a reject.";
    setText("chart-note", note);
  }

  function renderSql() {
    var error = document.getElementById("sql-error");
    var host = document.getElementById("sql-out");
    error.textContent = "";
    host.replaceChildren();
    if (!state.current) return;
    var query = document.getElementById("sql").value;
    try {
      var result = OrderSplit.runSql(query, state.current.run.rows);
      var table = document.createElement("table");
      var thead = document.createElement("thead");
      var hr = document.createElement("tr");
      result.columns.forEach(function (column) {
        var th = document.createElement("th");
        th.textContent = column;
        hr.appendChild(th);
      });
      thead.appendChild(hr);
      var tbody = document.createElement("tbody");
      result.rows.forEach(function (row) {
        var tr = document.createElement("tr");
        result.columns.forEach(function (column) {
          var value = row[column];
          var td = document.createElement("td");
          if (value === null || value === undefined) {
            td.textContent = t("nullWord");
            td.className = "null";
          } else if (typeof value === "number" && Number.isFinite(value)) {
            td.textContent = Number.isInteger(value) ? String(value) : String(Math.round(value * 1e6) / 1e6);
          } else {
            td.textContent = String(value);
          }
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
      table.appendChild(thead);
      table.appendChild(tbody);
      host.appendChild(table);
      var caption = document.createElement("p");
      caption.className = "note";
      caption.textContent = result.rows.length + (state.lang === "es" ? " filas." : " rows.");
      host.appendChild(caption);
    } catch (err) {
      error.textContent = err && err.message ? err.message : t("csvError");
    }
  }

  function checkMeasured() {
    var banner = document.getElementById("mismatch");
    var bad = state.samples.some(function (entry) {
      var measured = entry.measured;
      if (!measured) return false;
      if (entry.run.good !== measured.good || entry.run.reject !== measured.reject) return true;
      if (!measured.rows || measured.rows.length !== entry.run.rows.length) return true;
      return measured.rows.some(function (row, index) {
        var got = entry.run.rows[index];
        return got.line !== row.line || got.status !== row.verdict || got.reason !== row.reason;
      });
    });
    banner.hidden = !bad;
    banner.textContent = bad ? t("mismatch") : "";
  }

  function render() {
    applyStatic();
    if (!state.current) return;
    var run = state.current.run;
    renderHeadline(run);
    document.getElementById("lede").textContent = lede(run, state.current);
    document.getElementById("live").textContent = run.file_name + ". " + pair(run.good, run.reject) + ".";
    renderTable(run);
    renderGate(run);
    renderAlert(run);
    renderWildcard();
    renderBars();
    renderSql();
    checkMeasured();
    ["all", "good", "reject"].forEach(function (name) {
      document.getElementById("filter-" + name).setAttribute("aria-pressed", state.filter === name ? "true" : "false");
    });
  }

  function selectSample(name) {
    var entry = state.samples.find(function (item) { return item.name === name; });
    if (!entry) return;
    state.current = entry;
    document.getElementById("sample").value = name;
    render();
  }

  async function load() {
    applyStatic();
    try {
      var measuredResponse = await fetch("measured.json");
      if (!measuredResponse.ok) throw new Error("measured");
      var measured = await measuredResponse.json();
      var select = document.getElementById("sample");
      select.replaceChildren();
      var loaded = await Promise.all(measured.files.map(async function (file) {
        var response = await fetch("samples/" + encodeURIComponent(file.name));
        if (!response.ok) throw new Error(file.name);
        var text = await response.text();
        var option = document.createElement("option");
        option.value = file.name;
        option.textContent = file.name;
        select.appendChild(option);
        return {
          name: file.name,
          text: text,
          measured: file,
          run: OrderSplit.classifyText(file.name, text, utcToday())
        };
      }));
      state.samples = loaded;
      var preferred = loaded.find(function (item) { return item.name.indexOf("BestLocalPizza") === 0; }) || loaded[0];
      select.value = preferred.name;
      state.current = preferred;
      render();
    } catch (err) {
      document.getElementById("live").textContent = t("readError");
    }
  }

  document.getElementById("lang").addEventListener("click", function () {
    state.lang = state.lang === "es" ? "en" : "es";
    try { localStorage.setItem("pl-ordenes-lang", state.lang); } catch (err) { /* ignore */ }
    render();
  });

  document.getElementById("sample").addEventListener("change", function (event) {
    selectSample(event.target.value);
  });

  document.getElementById("run-sample").addEventListener("click", function () {
    var name = document.getElementById("sample").value;
    if (!name && state.samples[0]) name = state.samples[0].name;
    selectSample(name);
  });

  document.getElementById("upload").addEventListener("change", function (event) {
    var file = event.target.files && event.target.files[0];
    if (!file) return;
    var reader = new FileReader();
    reader.onload = function () {
      try {
        var text = String(reader.result);
        var run = OrderSplit.classifyText(file.name || "upload.csv", text, utcToday());
        state.current = { name: file.name || "upload.csv", text: text, run: run, uploaded: true, measured: null };
        render();
      } catch (err) {
        var message = err && err.message === "CSV has no header" ? t("noHeader") : t("csvError");
        document.getElementById("live").textContent = message;
      }
    };
    reader.readAsText(file);
  });

  document.getElementById("toolbar").addEventListener("submit", function (event) {
    event.preventDefault();
  });

  ["all", "good", "reject"].forEach(function (name) {
    document.getElementById("filter-" + name).addEventListener("click", function () {
      state.filter = name;
      ["all", "good", "reject"].forEach(function (item) {
        document.getElementById("filter-" + item).setAttribute("aria-pressed", state.filter === item ? "true" : "false");
      });
      if (state.current) renderTable(state.current.run);
    });
  });

  document.getElementById("run-sql").addEventListener("click", function () {
    renderSql();
  });

  document.getElementById("sql").addEventListener("keydown", function (event) {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") renderSql();
  });

  try {
    var saved = localStorage.getItem("pl-ordenes-lang");
    if (saved === "en" || saved === "es") state.lang = saved;
  } catch (err) { /* ignore */ }

  load();
})();
