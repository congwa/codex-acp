import {randomUUID} from "node:crypto";
import type {CodexAppServerClient, ExperimentalTurnStartParams} from "../CodexAppServerClient";
import type {Turn, TurnSteerParams, UserInput} from "../app-server/v2";
import {logger} from "../Logger";
import {record, WindowTransport, type RecordValue, type WindowService} from "./WindowTransport";

type PendingApproval = {
    readonly method: string;
    readonly params: RecordValue;
    resolve(decision: "accept" | "decline" | "cancel"): void;
};
interface OwnedThread {
    revision: number;
    followers: Set<string>;
    approvals: Map<string, PendingApproval>;
    sending: boolean;
    dirty: boolean;
    publishing: boolean;
    snapshotTail: Promise<unknown>;
    queue: RecordValue[];
    readonly dispatched: Set<string>;
}

const METHODS = new Map([
    ["thread-owner-discovery", 1],
    ["thread-follower-start-turn", 2],
    ["thread-follower-steer-turn", 1],
    ["thread-follower-interrupt-turn", 4],
    ["thread-follower-load-complete-history", 1],
    ["thread-follower-read-model-settings", 1],
    ["thread-follower-update-thread-settings", 2],
    ["thread-follower-remove-queued-message", 1],
    ["thread-follower-clear-queued-messages", 1],
    ["thread-follower-command-approval-decision", 1],
    ["thread-follower-file-approval-decision", 1],
]);

/** Publishes threads whose App Server subscription belongs to this adapter. */
export class WindowOwner implements WindowService {
    private readonly threads = new Map<string, OwnedThread>();
    private transport: WindowTransport | undefined;
    private timer: NodeJS.Timeout | undefined;
    private connecting = false;
    private disposed = false;
    private client: CodexAppServerClient | undefined;
    private unsubscribe: (() => void) | undefined;

    constructor(private readonly path: string, private readonly clientFor: () => CodexAppServerClient) {}

    register(id: string): void {
        if (!this.threads.has(id)) this.threads.set(id, {
            revision: 0, followers: new Set(), approvals: new Map(), sending: false, dirty: false, publishing: false, snapshotTail: Promise.resolve(), queue: [], dispatched: new Set(),
        });
        const client = this.clientFor();
        if (this.client !== client) {
            this.unsubscribe?.();
            this.client = client;
            this.unsubscribe = client.onClientTransportEvent(event => {
                if (event.eventType !== "notification") return;
                const params = record(event.params);
                const id = params?.["threadId"] ?? record(params?.["thread"])?.["id"];
                if (typeof id === "string") {
                    this.changed(id);
                    if (event.method === "turn/completed") void this.drainQueue(id);
                }
            });
        }
        this.reconnect(0);
    }

    release(id: string): void {
        const thread = this.threads.get(id);
        for (const approval of thread?.approvals.values() ?? []) approval.resolve("cancel");
        this.threads.delete(id);
    }

    close(): void {
        this.disposed = true;
        clearTimeout(this.timer);
        this.unsubscribe?.();
        for (const id of this.threads.keys()) this.release(id);
        this.transport?.close();
    }

    canHandle(request: RecordValue): boolean {
        const params = record(request["params"]);
        const id = params?.["conversationId"];
        const method = request["method"];
        const version = request["version"];
        const supported = method === "thread-follower-set-queued-follow-ups-state" ? version === 1 || version === 3
            : typeof method === "string" && METHODS.has(method) && METHODS.get(method) === version;
        return this.client === this.clientFor() && typeof id === "string" && this.threads.has(id)
            && (params?.["hostId"] === undefined || params["hostId"] === "local")
            && supported;
    }

    async handle(request: RecordValue): Promise<unknown> {
        const params = record(request["params"])!;
        const id = params["conversationId"] as string;
        const method = request["method"];
        const client = this.clientFor();
        const thread = this.threads.get(id)!;
        switch (method) {
            case "thread-owner-discovery": return {supportsUntrustedAppInput: false};
            case "thread-follower-load-complete-history":
                return {revision: await this.publish(id, thread, typeof request["sourceClientId"] === "string" ? [request["sourceClientId"]] : undefined)};
            case "thread-follower-read-model-settings": {
                const settings = client.getThreadSettings(id);
                return {settings: settings ? {...settings, reasoningEffort: settings.effort, resumeState: "resumed"} : null};
            }
            case "thread-follower-update-thread-settings": {
                const current = client.getThreadSettings(id);
                const settings = record(params["threadSettings"]);
                if (!current || !settings) return {applied: false};
                const model = settings["model"] ?? current.model;
                const effort = settings["effort"] ?? current.effort;
                if (typeof model !== "string" || (effort !== null && typeof effort !== "string")) return {applied: false};
                await client.threadSettingsUpdate({
                    threadId: id, collaborationMode: {mode: current.collaborationMode.mode,
                        settings: {model, reasoning_effort: effort, developer_instructions: current.collaborationMode.settings.developer_instructions}},
                });
                this.changed(id);
                return {applied: true};
            }
            case "thread-follower-start-turn": {
                const turn = record(record(params["turnStart"])?.["request"]);
                if (!turn || turn["threadId"] !== id || !Array.isArray(turn["input"])) throw new Error("invalid-turn-input");
                const messageId = turn["clientUserMessageId"];
                return {result: await this.startTurn(id, thread, turn["input"] as UserInput[],
                    typeof messageId === "string" && messageId ? messageId : randomUUID())};
            }
            case "thread-follower-set-queued-follow-ups-state": {
                const supplied = record(params["state"])?.[id];
                const discarded = new Set(Array.isArray(params["discardedMessageIds"]) ? params["discardedMessageIds"] : []);
                const messages = (Array.isArray(supplied) ? supplied : []).flatMap(value => {
                    const message = record(value);
                    const messageId = message?.["id"];
                    return message && typeof messageId === "string" && !discarded.has(messageId)
                        && !thread.dispatched.has(messageId) && typeof message["text"] === "string" ? [message] : [];
                });
                const preparingId = params["preparationMessageId"];
                if (typeof preparingId === "string") {
                    const prepared = messages.find(message => message["id"] === preparingId);
                    const previous = thread.queue.find(message => message["id"] === preparingId);
                    if (prepared && previous) thread.queue = thread.queue.map(message => message === previous ? prepared : message);
                    else if (prepared) thread.queue.push(prepared);
                } else thread.queue = messages;
                this.publishQueue(id, thread);
                void this.drainQueue(id);
                return {ok: true};
            }
            case "thread-follower-remove-queued-message": {
                const size = thread.queue.length;
                thread.queue = thread.queue.filter(message => message["id"] !== params["messageId"]);
                this.publishQueue(id, thread);
                return {removed: thread.queue.length !== size};
            }
            case "thread-follower-clear-queued-messages": {
                const messages = thread.queue;
                thread.queue = [];
                this.publishQueue(id, thread);
                return {messages};
            }
            case "thread-follower-steer-turn": {
                const turnId = client.runningTurnId(id);
                if (!turnId || !Array.isArray(params["input"])) throw new Error("no-running-turn");
                return {result: await client.turnSteer({
                    threadId: id, expectedTurnId: turnId, input: params["input"] as TurnSteerParams["input"],
                    ...(typeof params["clientUserMessageId"] === "string" ? {clientUserMessageId: params["clientUserMessageId"]} : {}),
                })};
            }
            case "thread-follower-interrupt-turn": {
                const turnId = client.runningTurnId(id);
                if (!turnId || turnId !== params["expectedTurnId"]) throw new Error("stale-turn");
                await client.turnInterrupt({threadId: id, turnId});
                return {ok: true, interruptedTurnId: turnId};
            }
            case "thread-follower-command-approval-decision":
            case "thread-follower-file-approval-decision": {
                const key = String(params["requestId"]);
                const approval = thread.approvals.get(key);
                const decision = params["decision"];
                const expectedMethod = method === "thread-follower-command-approval-decision"
                    ? "item/commandExecution/requestApproval" : "item/fileChange/requestApproval";
                if (approval?.method !== expectedMethod || !["accept", "decline", "cancel"].includes(String(decision))) {
                    throw new Error("stale-approval");
                }
                thread.approvals.delete(key);
                approval.resolve(decision as "accept" | "decline" | "cancel");
                this.changed(id);
                return {ok: true};
            }
            default: throw new Error("no-handler-for-request");
        }
    }

    private async startTurn(id: string, thread: OwnedThread, input: UserInput[], messageId: string) {
        const client = this.clientFor();
        if (thread.sending || client.runningTurnId(id)) throw new Error("turn-already-running");
        if (thread.dispatched.has(messageId)) throw new Error("message-already-dispatched");
        thread.dispatched.add(messageId);
        thread.sending = true;
        try {
            client.onApprovalRequest(id, {
                handleCommandExecution: async data => ({decision: await this.approve(id, thread, "item/commandExecution/requestApproval", data)}),
                handleFileChange: async data => ({decision: await this.approve(id, thread, "item/fileChange/requestApproval", data)}),
                handlePermissionsRequest: async () => ({permissions: {}, scope: "turn", strictAutoReview: false}),
            });
            return await client.turnStart({
                threadId: id, input, clientUserMessageId: messageId,
            } satisfies ExperimentalTurnStartParams);
        } finally {
            thread.sending = false;
            this.changed(id);
        }
    }

    private publishQueue(id: string, thread: OwnedThread): void {
        const params = {conversationId: id, hostId: "local", messages: thread.queue};
        this.transport?.broadcast("thread-queued-followups-changed", 2, params);
        this.transport?.broadcast("thread-queued-followups-changed", 3, params);
    }

    private async drainQueue(id: string): Promise<void> {
        const thread = this.threads.get(id);
        const message = thread?.queue[0];
        const client = this.clientFor();
        if (!thread || !message || this.client !== client || thread.sending || client.runningTurnId(id)
            || message["pausedReason"] || message["preparation"]) return;
        const context = record(message["context"]);
        if ([message["attachments"], context?.["fileAttachments"], context?.["uploadedFileAttachments"],
            context?.["pastedTextAttachments"], context?.["commentAttachments"]].some(value => Array.isArray(value) && value.length > 0)) return;
        const messageId = message["id"];
        const text = message["text"];
        if (typeof messageId !== "string" || typeof text !== "string") return;
        try {
            await this.startTurn(id, thread, [{type: "text", text, text_elements: []}], messageId);
            thread.queue = thread.queue.filter(value => value["id"] !== messageId);
        } catch (error) {
            // A failed RPC can still have landed. Keep it paused and do not dispatch it again.
            thread.queue = thread.queue.map(value => value["id"] === messageId ? {...value, pausedReason: "delivery-unknown"} : value);
            logger.error("Codex window queue delivery is unknown", error);
        }
        this.publishQueue(id, thread);
    }

    offerApproval(id: string, method: string, params: RecordValue, signal: AbortSignal): Promise<"accept" | "decline" | "cancel"> | undefined {
        const thread = this.threads.get(id);
        if (!thread || !thread.followers.size || !this.transport) return undefined;
        return this.approve(id, thread, method, params, signal);
    }

    private approve(id: string, thread: OwnedThread, method: string, params: RecordValue, signal?: AbortSignal): Promise<"accept" | "decline" | "cancel"> {
        if (this.threads.get(id) !== thread || thread.followers.size === 0) return Promise.resolve("cancel");
        const requestId = randomUUID();
        return new Promise(resolve => {
            const timeout = setTimeout(() => {
                thread.approvals.delete(requestId);
                resolve("cancel");
                this.changed(id);
            }, 5 * 60 * 1000);
            timeout.unref();
            const abort = () => {
                clearTimeout(timeout);
                thread.approvals.delete(requestId);
                resolve("cancel");
                this.changed(id);
            };
            thread.approvals.set(requestId, {method, params, resolve: decision => {
                clearTimeout(timeout);
                signal?.removeEventListener("abort", abort);
                resolve(decision);
            }});
            if (signal?.aborted) abort();
            else signal?.addEventListener("abort", abort, {once: true});
            this.changed(id);
        });
    }

    private reconnect(delay: number): void {
        if (this.timer || this.connecting || this.transport || this.disposed || this.threads.size === 0) return;
        this.timer = setTimeout(() => {
            this.timer = undefined;
            this.connecting = true;
            void WindowTransport.connect(this.path, event => this.event(event), () => {
                this.transport = undefined;
                for (const thread of this.threads.values()) thread.followers.clear();
                this.reconnect(1500);
            }, this).then(transport => {
                if (this.disposed) { transport.close(); return; }
                this.transport = transport;
                for (const id of this.threads.keys()) {
                    transport.broadcast("thread-stream-following-status-requested", 1, {conversationId: id, hostId: "local"});
                }
            }).catch(() => {}).finally(() => {
                this.connecting = false;
                this.reconnect(1500);
            });
        }, delay);
        this.timer.unref();
    }

    private event(event: RecordValue): void {
        const params = record(event["params"]);
        const source = event["sourceClientId"];
        if (event["method"] === "client-status-changed" && params?.["status"] === "disconnected") {
            for (const thread of this.threads.values()) thread.followers.delete(String(params["clientId"]));
            return;
        }
        if (event["method"] !== "thread-stream-following-changed" || event["version"] !== 1
            || params?.["hostId"] !== "local" || typeof source !== "string") return;
        const id = params["conversationId"];
        if (typeof id !== "string") return;
        const thread = this.threads.get(id);
        if (!thread) return;
        if (params["following"] === true) {
            thread.followers.add(source);
            this.changed(id);
        } else thread.followers.delete(source);
    }

    private changed(id: string): void {
        const thread = this.threads.get(id);
        if (!thread || thread.followers.size === 0) return;
        thread.dirty = true;
        if (thread.publishing) return;
        thread.publishing = true;
        void (async () => {
            while (thread.dirty && this.threads.get(id) === thread && thread.followers.size > 0) {
                thread.dirty = false;
                await this.publish(id, thread);
            }
        })().catch(error => logger.error("Codex window owner snapshot failed", error))
            .finally(() => { thread.publishing = false; if (thread.dirty) this.changed(id); });
    }

    private publish(id: string, owned: OwnedThread, targets?: string[]): Promise<number> {
        const pending = owned.snapshotTail.catch(() => {}).then(() => this.readAndPublish(id, owned, targets));
        owned.snapshotTail = pending;
        return pending;
    }

    private async readAndPublish(id: string, owned: OwnedThread, targets = [...owned.followers]): Promise<number> {
        const client = this.clientFor();
        const transport = this.transport;
        if (!transport || this.client !== client || this.threads.get(id) !== owned) throw new Error("owner-unavailable");
        const {thread} = await client.threadRead({threadId: id});
        const turns: Turn[] = [];
        if (!thread.ephemeral) {
            if (thread.historyMode === "paginated") {
                for await (const page of client.threadTurnPages({threadId: id, itemsView: "full", sortDirection: "asc", limit: 25})) turns.push(...page);
            } else {
                turns.push(...(await client.threadRead({threadId: id, includeTurns: true})).thread.turns);
            }
        }
        if (this.transport !== transport || this.clientFor() !== client || this.threads.get(id) !== owned) throw new Error("owner-unavailable");
        const active = client.runningTurnId(id);
        if (active && !turns.some(turn => turn.id === active)) {
            turns.push({id: active, status: "inProgress", items: [], itemsView: "full", error: null,
                rootTurnId: null, startedAt: null, completedAt: null, durationMs: null});
        }
        const entries = turns.map(turn => ({key: `turn:${turn.id}`, value: `turn:${turn.id}`}));
        const entitiesByKey = Object.fromEntries(turns.map(turn => {
            const user = turn.items.find(item => item.type === "userMessage");
            return [`turn:${turn.id}`, {
                params: {threadId: id, input: user?.content ?? [], turnTrigger: "composer", clientUserMessageId: user?.clientId ?? null},
                permissionParamsSource: "inferred", turnId: turn.id,
                turnStartedAtMs: turn.startedAt === null ? null : turn.startedAt * 1000,
                durationMs: turn.durationMs, status: turn.status, error: turn.error, diff: null, items: turn.items,
                completedItemIds: turn.status === "inProgress" ? {} : Object.fromEntries(turn.items.map(item => [item.id, true])),
                itemsPagination: {olderCursor: null, isLoadingOlder: false, hasLoadedOldest: true, newestSnapshotItemId: turn.items.at(-1)?.id ?? null},
                localMetadata: {}, mcpAppModelContextAttachments: [], hookRuns: [],
            }];
        }));
        const settings = client.getThreadSettings(id);
        const oldest = turns[0]?.id ?? null;
        const conversationState = {
            ...thread, hostId: "local", title: thread.name, rolloutPath: thread.path, resumeState: "resumed",
            threadRuntimeStatus: thread.status,
            latestModel: settings?.model ?? null,
            latestReasoningEffort: settings?.effort ?? null,
            latestThreadSettings: settings ?? null,
            latestCollaborationMode: settings?.collaborationMode ?? null,
            currentPermissions: settings ? {approvalPolicy: settings.approvalPolicy, approvalsReviewer: settings.approvalsReviewer,
                sandboxPolicy: settings.sandboxPolicy} : null,
            mode: settings?.collaborationMode.mode ?? "default",
            environments: [{environmentId: "local", cwd: thread.cwd, runtimeWorkspaceRoots: [thread.cwd]}],
            turnsPagination: {olderCursor: null, oldestLoadedTurnId: oldest, isLoadingOlder: false, hasLoadedOldest: true},
            paginatedHistory: {itemsBackwardsCursor: null},
            turns: [], turnHistory: {kind: "canonical", history: {entitiesByKey, generation: 0, isComplete: true, islands: [{
                id: "tail:0", entries, olderBoundary: {status: "exhausted", boundaryId: "tail:0:older"},
                newerBoundary: {status: "exhausted", boundaryId: "tail:0:newer"},
            }]}},
            requests: [...owned.approvals].map(([requestId, approval]) => ({id: requestId, requestId, method: approval.method, params: approval.params})),
        };
        const revision = ++owned.revision;
        transport.broadcast("thread-stream-state-changed", 11, {
            conversationId: id, hostId: "local", change: {type: "snapshot", revision, conversationState},
        }, targets);
        return revision;
    }
}
