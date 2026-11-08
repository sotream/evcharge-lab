/** Library entry point (the process entry is main.ts): lets tests and drills run the simulator in-process. */
export { createApi } from './api';
export { loadSimConfig, simConfigSchema, type SimConfig } from './config';
export { buildFleet, stationSpec, type StationSpec } from './fleet';
export { Simulator } from './simulator';
