import {randomUUID} from "node:crypto";
import {WindowFollower} from "./WindowFollower";
import {windowTurns, windowUserItems, type WindowSnapshot} from "./WindowSnapshot";
import {record, WindowRequestError, type RecordValue} from "./WindowTransport";

export interface WindowTurn {
    id: string;
    status: string;
}

export function turnOf(turn: RecordValue): WindowTurn | undefined {
    const id = turn["turnId"] ?? turn["id"];
    const status = turn["status"];
    return typeof id === "string" && typeof status === "string" ? {id, status} : undefined;
}

const isActive = (turn: WindowTurn) => turn.status === "inProgress" || turn.status === "running";

type Receipt = {text: string; mode: "start" | "steer"; result: Promise<WindowTurn>};

/** Holds a single thread's route and input receipts. Writes always target the observed window owner. */
export class WindowSession {
    readonly follower: WindowFollower;
    private readonly receipts = new Map<string, Receipt>();
    private reconnect: NodeJS.Timeout | undefined;
    private attached = false;
    private openingWrite = false;
    private unresolvedInput: string | undefined;

    constructor(path: string, readonly id: string, readonly cwd: string, onChange: (snapshot: WindowSnapshot | undefined) => void) {
        this.follower = new WindowFollower(path, id, snapshot => {
            if (this.unresolvedInput && this.findInput(this.unresolvedInput)) this.unresolvedInput = undefined;
            if (this.attached) onChange(snapshot);
            if (!snapshot && this.attached) this.scheduleReconnect();
        });
    }

    get snapshot(): WindowSnapshot | undefined { return this.follower.snapshot; }

    async open(): Promise<WindowSnapshot | null> { return this.follower.open(); }

    activate(): void { this.attached = true; }

    close(): void {
        this.attached = false;
        clearTimeout(this.reconnect);
        this.follower.close();
    }

    activeTurn(): WindowTurn | undefined {
        if (!this.snapshot) return undefined;
        const turn = windowTurns(this.snapshot.state).at(-1);
        const current = turn && turnOf(turn);
        return current && isActive(current) ? current : undefined;
    }

    submit(text: string, mode: "start" | "steer", messageId: string = randomUUID(), signal?: AbortSignal): Promise<WindowTurn> {
        const previous = this.receipts.get(messageId);
        if (previous) {
            if (previous.text !== text || previous.mode !== mode) return Promise.reject(new Error("Message ID was already used for another input"));
            // A canonical message can settle a previous unknown receipt.
            const observed = this.findInput(messageId);
            return observed ? Promise.resolve(observed.turn) : previous.result;
        }
        const canonical = this.findInput(messageId);
        if (canonical) {
            if (canonical.text !== text) return Promise.reject(new Error("Message ID already belongs to another input"));
            return Promise.resolve(canonical.turn);
        }
        if (this.openingWrite || this.unresolvedInput) return Promise.reject(new WindowRequestError("another input is awaiting confirmation", false));
        if (this.receipts.size >= 1024) {
            const oldest = this.receipts.keys().next().value;
            if (oldest !== undefined) this.receipts.delete(oldest);
        }
        this.openingWrite = true;
        const result = this.dispatch(text, mode, messageId, signal).catch(error => {
            if (!(error instanceof WindowRequestError && error.deliveryUnknown)) this.receipts.delete(messageId);
            throw error;
        }).finally(() => { this.openingWrite = false; });
        this.receipts.set(messageId, {text, mode, result});
        return result;
    }

    private async dispatch(text: string, mode: "start" | "steer", messageId: string, signal?: AbortSignal): Promise<WindowTurn> {
        if (signal?.aborted) throw new WindowRequestError("request-cancelled", false);
        const snapshot = this.snapshot;
        if (!snapshot) throw new WindowRequestError("owner-unavailable", false);
        const active = this.activeTurn();
        if (mode === "start" && active) throw new WindowRequestError("turn-already-running", false);
        if (mode === "steer" && !active) throw new WindowRequestError("no-running-turn", false);
        const input = [{type: "text", text, text_elements: []}];
        const settings = record(snapshot.state["latestThreadSettings"]);
        const permissions = record(snapshot.state["currentPermissions"]);
        const response = mode === "start"
            ? this.follower.control("thread-follower-start-turn", 2, {
                turnStart: {
                    request: {
                        threadId: this.id, cwd: this.cwd, input, clientUserMessageId: messageId,
                        turnTrigger: "composer", summary: "none",
                        approvalPolicy: settings?.["approvalPolicy"] ?? permissions?.["approvalPolicy"] ?? null,
                        approvalsReviewer: settings?.["approvalsReviewer"] ?? permissions?.["approvalsReviewer"] ?? null,
                        sandboxPolicy: settings?.["sandboxPolicy"] ?? permissions?.["sandboxPolicy"] ?? null,
                        collaborationMode: snapshot.state["latestCollaborationMode"] ?? null,
                    },
                    context: {inheritThreadSettings: true},
                },
            })
            : this.follower.control("thread-follower-steer-turn", 1, {
                input, clientUserMessageId: messageId, attachments: [],
                restoreMessage: {id: messageId, cwd: this.cwd, text, context: {commentAttachments: [], workspaceRoots: [this.cwd]}},
            });
        const observe = new AbortController();
        try {
            return await Promise.race([
                this.follower.waitFor(() => this.findInput(messageId)?.turn, 20000, observe.signal),
                response.then(result => {
                    const value = record(record(result)?.["result"] ?? result);
                    const turn = record(value?.["turn"]);
                    const accepted = turn && turnOf(turn);
                    return accepted ?? (mode === "steer" && active ? active : new Promise<never>(() => {}));
                }).catch(error => {
                    if (error instanceof WindowRequestError && !error.deliveryUnknown) throw error;
                    // The window can apply the request and fail to return its reply.
                    return new Promise<never>(() => {});
                }),
            ]);
        } catch (error) {
            if (error instanceof WindowRequestError && error.deliveryUnknown) this.unresolvedInput = messageId;
            throw error;
        } finally {
            observe.abort();
        }
    }

    completedReason(id: string): "end_turn" | "cancelled" | "_codex_window_failed" {
        const native = this.snapshot && windowTurns(this.snapshot.state).map(turnOf).find(turn => turn?.id === id);
        return native?.status === "completed" ? "end_turn" : native?.status === "failed" ? "_codex_window_failed" : "cancelled";
    }

    waitForEnd(turn: WindowTurn, signal?: AbortSignal): Promise<WindowTurn> {
        return this.follower.waitFor(() => {
            if (!this.snapshot) return undefined;
            const native = windowTurns(this.snapshot.state).map(turnOf).find(value => value?.id === turn.id);
            return native && !isActive(native) ? native : undefined;
        }, 30 * 60 * 1000, signal);
    }

    async cancel(): Promise<void> {
        if (!this.snapshot) throw new WindowRequestError("owner-unavailable", false);
        const active = this.activeTurn();
        if (!active) return;
        await this.follower.control("thread-follower-interrupt-turn", 4, {mode: "user-stop", expectedTurnId: active.id});
    }

    private findInput(id: string): {turn: WindowTurn; text: string} | undefined {
        if (!this.snapshot) return undefined;
        for (const turn of windowTurns(this.snapshot.state)) {
            const native = turnOf(turn);
            if (!native) continue;
            const input = windowUserItems(turn).map(record).find(item => item?.["type"] === "userMessage" && item["clientId"] === id);
            if (!input) continue;
            const content = input["content"];
            const text = Array.isArray(content) ? content.map(record)
                .filter(block => block?.["type"] === "text")
                .map(block => String(block?.["text"] ?? "")).join("\n") : "";
            return {turn: native, text};
        }
        return undefined;
    }

    private scheduleReconnect(): void {
        if (this.reconnect) return;
        this.reconnect = setTimeout(() => {
            this.reconnect = undefined;
            if (!this.attached || this.snapshot) return;
            void this.open().catch(() => null).then(snapshot => {
                if (snapshot === null && this.attached) this.scheduleReconnect();
            });
        }, 1000);
        this.reconnect.unref();
    }
}
