import {WindowFollower} from "./WindowFollower";
import {record, type RecordValue} from "./WindowTransport";

export interface WindowSnapshot {
    readonly revision: number;
    readonly state: RecordValue;
}

export async function readWindowSnapshot(path: string, conversationId: string): Promise<WindowSnapshot | null> {
    const follower = new WindowFollower(path, conversationId, () => {});
    try {
        return await follower.open();
    } finally {
        follower.close();
    }
}

/** The owner's turn history is ordered by island and entry, not by object keys. */
export function windowTurns(state: RecordValue): RecordValue[] {
    const history = record(record(state["turnHistory"])?.["history"]);
    const entities = record(history?.["entitiesByKey"]);
    if (entities === undefined) {
        return (Array.isArray(state["turns"]) ? state["turns"] : []).flatMap(turn => {
            const value = record(turn);
            return value ? [value] : [];
        });
    }
    const keys = new Set<string>();
    for (const island of Array.isArray(history?.["islands"]) ? history["islands"] : []) {
        const entries = record(island)?.["entries"];
        for (const entry of Array.isArray(entries) ? entries : []) {
            const key = record(entry)?.["key"];
            if (typeof key === "string") keys.add(key);
        }
    }
    for (const key of Object.keys(entities)) keys.add(key);
    return [...keys].flatMap(key => {
        const turn = record(entities[key]);
        return turn === undefined ? [] : [{...turn, id: turn["turnId"] ?? turn["id"] ?? key}];
    });
}

export function windowHistoryItems(state: RecordValue): unknown[] {
    return windowTurns(state).flatMap(turn => windowUserItems(turn));
}

export function windowUserItems(turn: RecordValue): unknown[] {
    const items = Array.isArray(turn["items"]) ? turn["items"] : [];
    const steered = new Set(items.map(record).filter(item => item?.["type"] === "steered").map(item => item?.["id"]));
    return items.flatMap(raw => {
        const item = record(raw);
        if (item?.["type"] !== "steeringUserMessage") return item?.["type"] === "steered" ? [] : [raw];
        const serverId = item["serverUserMessageId"];
        if (typeof serverId !== "string" || (item["status"] !== "accepted" && !steered.has(serverId))) return [];
        return [{type: "userMessage", id: serverId, content: item["input"], clientId: item["serverClientUserMessageId"] ?? item["clientUserMessageId"] ?? null}];
    });
}
