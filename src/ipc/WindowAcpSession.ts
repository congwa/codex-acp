import {createModelConfigOption, createReasoningEffortConfigOption} from "../ModelConfigOption";
import type {Model} from "../app-server/v2";
import * as acp from "@agentclientprotocol/sdk/unstable";
import {ACPSessionConnection, type AcpClientConnection, type UpdateSessionEvent} from "../ACPSessionConnection";
import {logger} from "../Logger";
import {record, WindowRequestError} from "./WindowTransport";
import {WindowSession} from "./WindowSession";
import {windowHistoryItems, type WindowSnapshot} from "./WindowSnapshot";
import type {ThreadItem} from "../app-server/v2";

export function windowError(error: unknown): acp.RequestError {
    if (error instanceof acp.RequestError) return error;
    const unknown = error instanceof WindowRequestError && error.deliveryUnknown;
    return new acp.RequestError(-32080, unknown ? "Codex window delivery is unknown" : "Codex window session is unavailable", {
        code: unknown ? "codex_ipc_outcome_unknown" : "codex_ipc_unavailable",
        detail: error instanceof Error ? error.message : String(error),
    });
}

export class WindowAcpSession {
    readonly session: WindowSession;
    private readonly output: ACPSessionConnection;
    private readonly seen = new Map<string, string>();
    private readonly replayEntities = new Set<string>();
    private readonly interactions = new Map<string, AbortController>();
    private queued: Promise<void> = Promise.resolve();
    private closed = false;
    private running: string | undefined;
    private stateKnown = false;
    private settings = "";
    private advertisedState: string | undefined;
    models: Model[] = [];
    private interactionEpoch: number | undefined;

    constructor(
        path: string, id: string, cwd: string, private readonly connection: AcpClientConnection,
        private readonly render: (item: ThreadItem) => Promise<UpdateSessionEvent[]>,
    ) {
        this.output = new ACPSessionConnection(connection, id);
        this.session = new WindowSession(path, id, cwd, snapshot => this.enqueue(snapshot));
    }

    async initialize(replay: boolean): Promise<boolean> {
        const snapshot = await this.session.open();
        if (!snapshot) return false;
        await this.flush();
        if (replay) {
            this.seen.clear();
            this.replayEntities.clear();
        }
        await this.project(snapshot, replay, true);
        return true;
    }

    activate(): void {
        this.session.activate();
        this.enqueue(this.session.snapshot);
    }

    close(): void {
        this.closed = true;
        this.session.close();
        for (const controller of this.interactions.values()) controller.abort();
        this.interactions.clear();
    }

    async flush(): Promise<void> { await this.queued; }

    configOptions(models: Model[]): acp.SessionConfigOption[] {
        const state = this.session.snapshot?.state;
        const settings = record(state?.["latestThreadSettings"]);
        const model = settings?.["model"] ?? state?.["latestModel"];
        const effort = settings?.["effort"] ?? state?.["latestReasoningEffort"];
        if (typeof model !== "string" || !model) return [];
        const options = [createModelConfigOption(models, model)];
        const available = models.find(value => value.id === model || value.model === model)?.supportedReasoningEfforts ?? [];
        if (typeof effort === "string" && available.some(value => value.reasoningEffort === effort)) {
            options.push(createReasoningEffortConfigOption(available, effort));
        }
        return options;
    }

    async configure(configId: string, value: string, models: Model[]): Promise<acp.SessionConfigOption[]> {
        const option = this.configOptions(models).find(option => option.id === configId);
        if (!option || option.type === "boolean" || !option.options.some(entry => "value" in entry && entry.value === value)) {
            throw acp.RequestError.invalidParams(undefined, "Unsupported Codex window option");
        }
        if (option.currentValue === value) return this.configOptions(models);
        const key = configId === "model" ? "model" : "effort";
        const result = record(await this.session.follower.control("thread-follower-update-thread-settings", 2, {threadSettings: {[key]: value}}));
        if (result?.["applied"] !== true) throw new Error("Codex window did not apply the configuration");
        return await this.session.follower.waitFor(() => {
            const options = this.configOptions(models);
            return options.some(item => item.id === configId && item.currentValue === value) ? options : undefined;
        }, 6000);
    }


    private enqueue(snapshot: WindowSnapshot | undefined): void {
        const epoch = this.session.follower.epoch;
        this.queued = this.queued.then(async () => {
            if (!this.closed && epoch === this.session.follower.epoch) await this.project(snapshot, false, false);
        }).catch(error => logger.error("Could not project Codex window state", error));
    }

    private async project(snapshot: WindowSnapshot | undefined, replay: boolean, setup: boolean): Promise<void> {
        if (snapshot) {
            for (const raw of windowHistoryItems(snapshot.state)) {
                const value = record(raw);
                if (!value || typeof value["id"] !== "string" || typeof value["type"] !== "string") continue;
                const item = value as ThreadItem;
                const previous = this.seen.get(item.id);
                const serialized = JSON.stringify(item);
                if (previous === serialized) continue;
                if (setup && !replay) {
                    this.seen.set(item.id, serialized);
                    continue;
                }
                let rendered = item;
                if (previous && item.type === "agentMessage") {
                    const earlier = (JSON.parse(previous) as {text?: string}).text ?? "";
                    // Appends map to ACP deltas. Rewrites require a fresh replay rather than a duplicate chunk.
                    if (!item.text.startsWith(earlier)) continue;
                    rendered = {...item, text: item.text.slice(earlier.length)};
                } else if (previous && (item.type === "userMessage" || item.type === "reasoning")) continue;
                for (const update of await this.render(rendered) ?? []) {
                    if (this.closed) return;
                    if (replay) await this.output.replayUpdate(update, this.replayEntities);
                    else if (previous && update.sessionUpdate === "tool_call") {
                        const {sessionUpdate: _, ...details} = update;
                        await this.output.update({...details, sessionUpdate: "tool_call_update"});
                    } else if (item.type === "userMessage" && update.sessionUpdate === "user_message_chunk") {
                        await this.output.update({...update, _meta: {...update._meta, "aa/windowMessage": {id: item.id, clientId: item.clientId}}});
                    } else await this.output.update(update);
                }
                this.seen.set(item.id, serialized);
            }
        }
        if (setup) return;
        const activity = !snapshot ? "unavailable" : this.session.activeTurn() ? "running" : "idle";
        if (this.output.protocolVersion === 1 && activity !== this.advertisedState) {
            this.advertisedState = activity;
            await this.output.update({sessionUpdate: "session_info_update", _meta: {"aa/windowActivity": activity}});
        }
        this.syncApprovals(snapshot);
        if (snapshot) {
            const configOptions = this.configOptions(this.models);
            const settings = JSON.stringify(configOptions);
            if (settings !== this.settings) {
                this.settings = settings;
                if (configOptions.length) await this.output.update({sessionUpdate: "config_option_update", configOptions});
            }
        }
        if (this.output.protocolVersion === 2) {
            const current = this.session.activeTurn()?.id;
            if (!snapshot && this.stateKnown) {
                this.stateKnown = false;
                await this.output.updateState({state: "unknown"});
            } else if (snapshot && (!this.stateKnown || current !== this.running)) {
                const previous = this.running;
                this.stateKnown = true;
                this.running = current;
                const reason = previous ? this.session.completedReason(previous) : null;
                if (current) await this.output.updateState({state: "running"});
                else if (reason === "_codex_window_failed") {
                    await this.output.updateState({state: "idle", stopReason: "error", error: {code: -32080, message: "Codex window turn failed"}});
                } else if (reason === "cancelled") await this.output.updateState({state: "idle", stopReason: "cancelled"});
                else if (reason === "end_turn") await this.output.updateState({state: "idle", stopReason: "end_turn"});
                else await this.output.updateState({state: "idle"});
            }
        }
    }

    private syncApprovals(snapshot: WindowSnapshot | undefined): void {
        const requests = snapshot?.state["requests"];
        const pending = new Set<string>();
        const epoch = this.session.follower.epoch;
        if (this.interactionEpoch !== epoch) {
            for (const controller of this.interactions.values()) controller.abort();
            this.interactions.clear();
            this.interactionEpoch = epoch;
        }
        for (const value of Array.isArray(requests) ? requests : []) {
            const request = record(value);
            const id = request?.["requestId"] ?? request?.["id"];
            const method = request?.["method"];
            if ((typeof id !== "string" && typeof id !== "number") || typeof method !== "string") continue;
            const target = method === "item/commandExecution/requestApproval" ? "command" :
                method === "item/fileChange/requestApproval" ? "file" : undefined;
            if (!target) continue;
            const key = String(id);
            pending.add(key);
            if (this.interactions.has(key)) continue;
            const controller = new AbortController();
            this.interactions.set(key, controller);
            const params = record(request?.["params"]);
            const itemId = params?.["itemId"];
            const title = params?.["command"] ?? params?.["reason"];
            void this.connection.request(acp.methods.client.session.requestPermission, {
                sessionId: this.session.id,
                toolCall: {
                    toolCallId: typeof itemId === "string" ? itemId : key,
                    title: typeof title === "string" ? title : "Codex approval",
                    kind: target === "command" ? "execute" : "edit",
                },
                options: [
                    {optionId: "accept", name: "Allow once", kind: "allow_once"},
                    {optionId: "decline", name: "Reject", kind: "reject_once"},
                ],
            }, {cancellationSignal: controller.signal}).then(async response => {
                if (controller.signal.aborted || response.outcome.outcome !== "selected") return;
                const latest = this.session.snapshot?.state["requests"];
                if (!Array.isArray(latest) || !latest.some(value => (record(value)?.["requestId"] ?? record(value)?.["id"]) === id)) return;
                await this.session.follower.control(`thread-follower-${target}-approval-decision`, 1, {
                    requestId: id, decision: response.outcome.optionId === "accept" ? "accept" : "decline",
                }, epoch);
            }).catch(error => {
                if (!controller.signal.aborted) logger.error("Codex window approval failed", error);
            });
        }
        for (const [id, controller] of this.interactions) {
            if (!pending.has(id)) {
                controller.abort();
                this.interactions.delete(id);
            }
        }
    }
}
