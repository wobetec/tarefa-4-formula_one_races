/*  ==========================================================================
dataLoader.js
--------------------------------------------------------------------------
Módulo centralizado de leitura de CSVs com D3 v7.
Fornece a função assíncrona loadData() que devolve
todos os datasets já tipados (via d3.autoType) e
alguns mapas auxiliares para lookups rápidos.
========================================================================== */

import * as d3 from 'd3';

// static/data/**   (path absoluto na app)
const DATA_PATH = '/data';

export async function loadData(base) {
  /* -----------------------------------------------------------------------
    * 1. Lê todos os CSVs em paralelo
    * --------------------------------------------------------------------- */
  const [
    drivers,
    driverStandings,
    constructors,
    constructorStandings,
    races
  ] = await Promise.all([
    d3.csv(`${base}${DATA_PATH}/drivers.csv`, d3.autoType),
    d3.csv(`${base}${DATA_PATH}/drivers_standings.csv`, d3.autoType),
    d3.csv(`${base}${DATA_PATH}/constructors.csv`, d3.autoType),
    d3.csv(`${base}${DATA_PATH}/constructors_standings.csv`, d3.autoType),
    d3.csv(`${base}${DATA_PATH}/races.csv`, d3.autoType)
  ]);

  /* -----------------------------------------------------------------------
    * 2. Pré‑processamentos úteis
    * --------------------------------------------------------------------- */

  // Mapa "season-round"  →  registro da corrida
  const raceKeyMap = new Map(
    races.map((d) => [`${d.season}-${d.round}`, d])
  );

  // Mapa driverId → nome completo
  const driverNames = new Map(
    drivers.map((d) => [d.driverId, `${d.givenName} ${d.familyName}`])
  );

  // Mapa constructorId → nome
  const constructorNames = new Map(
    constructors.map((d) => [d.constructorId, d.name])
  );

  // Junta nomes nos standings (evita join toda vez que o gráfico é redesenhado)
  driverStandings.forEach((d) => (d.driver = driverNames.get(d.driverId)));
  constructorStandings.forEach(
    (d) => (d.constructor = constructorNames.get(d.constructorId))
  );

  // Retorno “gordo” (sem esconder nada)
  return {
    // coleções cruas
    drivers,
    driverStandings,
    constructors,
    constructorStandings,
    races,

    // “views” auxiliares
    raceKeyMap,
    driverNames,
    constructorNames
  };
}
