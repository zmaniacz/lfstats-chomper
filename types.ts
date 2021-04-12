interface lfstats {
  lfstatsId: number | null;
}

export enum DeacType {
  Resupply = "RESUPPLY",
  Nuke = "NUKE",
  Opponent = "OPPONENT",
  Team = "TEAM",
  Penalty = "PENALTY",
}

export enum EntityType {
  Commander = "Commander",
  Heavy = "Heavy Weapons",
  Scout = "Scout",
  Ammo = "Ammo Carrier",
  Medic = "Medic",
}

export interface Game extends lfstats {
  missionType: string;
  missionDesc: string;
  missionStart: number;
  missionStartTime: string;
  missionDuration: number;
  missionDurationMillis: number;
  missionLength: number | null;
  missionLengthMillis: number | null;
  penaltyValue: number;
}

export interface GameMetaData {
  fileVersion: string;
  programVersion: string;
  regionCode: string;
  siteCode: string;
  chomperVersion: string;
  tdfKey: string;
}

export interface Team extends lfstats {
  index: number;
  desc: string;
  colorEnum: number;
  colorDesc: string;
}

export interface Entity extends lfstats {
  startTime: number;
  ipl_id: string;
  type: string;
  desc: string;
  team: number;
  level: number;
  category: number;
  position: EntityType;
  maxLives: number;
  maxShots: number;
  resupplyLives: number;
  resupplyShots: number;
  shotPower: number;
  maxHP: number;
  battlesuit: string | null;
  endCode: number | null;
  initialState: EntityState | null;
  finalState: EntityState | null;
}

export interface EntityState {
  stateTime: number; //timestamp for the state
  isFinal: boolean; //boolean to idnicate if this is the entity's final state at game end
  ipl_id: string; //iplaylaserforce id string
  score: number; //score
  isActive: boolean; //is the entity currently activated
  isNuking: boolean; //is the entity currently nuking
  lives: number; //curent lives total
  shots: number; //current shots total
  currentHP: number; //curent HP total
  lastDeacTime: number | null; //timestamp of the alst deac
  lastDeacType: DeacType | null; //type of last deac
  isRapidActive: boolean; //is the entity currently in rapid
  shotsFired: number; //total shots fired
  shotsHit: number; //total shots that hit anything
  shotTeam: number; //total shots that hit the same team
  deacTeam: number; //total times a team mate was deactivated
  shot3Hit: number; //total times an opposing 3 hit was shot
  deac3Hit: number; //total times an opposing 3 hit was deactivated
  shotOpponent: number; //total times an opponent was shot
  deacOpponent: number; //total times an opponent was deactivated
  shotBase: number; //total times abses or gens were shot
  destroyBase: number; //total times bases org ens were destroyed
  medicHits: number; //total times the opposing medic was hit
  ownMedicHits: number; //total times a teammate medic was hit
  selfHit: number; //total times the player was hit form any source
  selfDeac: number; //total times the payer was deactivated form any source
  missileBase: number; //total times a base or gen was missiled
  missileTeam: number; //total times a teammate was missiled
  missileOpponent: number; //total time an oppionent was misisled
  missiles: number; //current missiles remaining
  selfMissile: number; //total times player was missiled
  spSpent: number; //total special points spent
  spEarned: number; //total special poitns eanred
  ammoBoosts: number; //total ammo boosts pulled
  lifeBoosts: number; //total life boosts pulled
  ammoBoostedPlayers: number; //total players that received an ammo boost form this player
  lifeBoostedPlayers: number; //total players that received a life boost form this player
  rapidFires: number; //total rapid fires activated
  shotsFiredDuringRapid: number;
  shotsHitDuringRapid: number;
  shotTeamDuringRapid: number;
  deacTeamDuringRapid: number;
  shot3HitDuringRapid: number;
  deac3HitDuringRapid: number;
  shotOpponentDuringRapid: number;
  deacOpponentDuringRapid: number;
  medicHitsDuringRapid: number;
  selfHitDuringRapid: number;
  selfDeacDuringRapid: number;
  nukesActivated: number;
  nukesDetonated: number;
  nukeMedicHits: number;
  ownNukeCanceledByNuke: number;
  ownNukeCanceledByGameEnd: number;
  ownNukeCanceledByTeam: number;
  ownNukeCanceledByResupply: number;
  ownNukeCanceledByOpponent: number;
  ownNukeCanceledByPenalty: number;
  ammoBoostReceieved: number;
  lifeBoostReceived: number;
  cancelOpponentNuke: number;
  cancelTeamNuke: number;
  cancelTeamNukeByResupply: number;
  uptime: number;
  resupplyDowntime: number;
  nukeDowntime: number;
  teamDeacDowntime: number;
  oppDeacDowntime: number;
  penaltyDowntime: number;
  penalties: number;
}

export interface GameAction {
  time: number;
  type: string;
  player: string | null;
  action: string | null;
  target: string | null;
  state?: Map<string, EntityState>;
}

export interface EntityDefault {
  shots: number;
  maxShots: number;
  resupplyShots: number;
  lives: number;
  maxLives: number;
  resupplyLives: number;
  missilesLeft: number;
  shotPower: number;
  currentHP: number;
  maxHP: number;
}
