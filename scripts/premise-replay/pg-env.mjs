// libpq receives decoded credentials as environment data, never shell source.
import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";

export function pgEnvironment(url, inherited = process.env) {
  const u = new URL(url);
  if (!["postgres:", "postgresql:"].includes(u.protocol)) throw new Error("expected a PostgreSQL URL");
  return {
    ...inherited,
    PGHOST: u.hostname,
    PGPORT: u.port || "5432",
    PGUSER: decodeURIComponent(u.username),
    PGPASSWORD: decodeURIComponent(u.password),
    PGDATABASE: decodeURIComponent(u.pathname.slice(1)),
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    process.stdout.write(execFileSync("psql", process.argv.slice(2), {
      env: pgEnvironment(process.env.REPLAY_DATABASE_URL),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }));
  } catch (error) {
    // Do not print the Error object: it includes the child environment.
    process.stderr.write(error.stderr || "psql invocation failed\n");
    process.exitCode = Number.isInteger(error.status) ? error.status : 1;
  }
}
