import {lstat, realpath} from "node:fs/promises";
import {createConnection} from "node:net";
import {isAbsolute} from "node:path";
import {PassThrough} from "node:stream";
import {once} from "node:events";
import WebSocket from "ws";
import * as rpc from "vscode-jsonrpc/node";
import {createJSONRPCReader} from "../StdUtils";

export async function connectSharedAppServer(path: string): Promise<rpc.MessageConnection> {
    if (!isAbsolute(path) || process.getuid === undefined) {
        throw new Error("CODEX_ACP_DAEMON_SOCKET requires an absolute Unix socket path");
    }
    const socketPath = await realpath(path);
    const socketFile = await lstat(socketPath);
    if (!socketFile.isSocket() || socketFile.uid !== process.getuid() || (socketFile.mode & 0o077) !== 0) {
        throw new Error("The Codex daemon socket must be private and owned by the current user");
    }
    const websocket = new WebSocket("ws://localhost/rpc", {
        createConnection: () => createConnection(socketPath),
        handshakeTimeout: 5000,
        maxPayload: 64 * 1024 * 1024,
    });
    await once(websocket, "open");
    const incoming = new PassThrough();
    const closed = new rpc.Emitter<void>();
    const errors = new rpc.Emitter<[Error, rpc.Message | undefined, number | undefined]>();
    const connection = rpc.createMessageConnection(createJSONRPCReader(incoming), {
        onError: errors.event,
        onClose: closed.event,
        async write(message) {
            await new Promise<void>((resolve, reject) => {
                websocket.send(JSON.stringify(message), error => error ? reject(error) : resolve());
            });
        },
        end() { websocket.close(); },
        dispose() { websocket.close(); },
    });
    websocket.on("message", (message, isBinary) => {
        if (isBinary) {
            websocket.close(1003, "JSON-RPC requires a text frame");
            return;
        }
        incoming.write(message.toString() + "\n");
    });
    websocket.on("error", error => errors.fire([error, undefined, undefined]));
    websocket.once("close", () => {
        incoming.end();
        closed.fire();
        connection.dispose();
        closed.dispose();
        errors.dispose();
    });
    connection.listen();
    return connection;
}
