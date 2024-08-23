import {DatabaseSync} from "node:sqlite";
import {DB} from "@abaplint/runtime";

export class SQLiteDatabaseClient implements DB.DatabaseClient {
  public readonly name = "sqlite";
  private readonly trace: boolean;
  private sqlite: DatabaseSync | undefined = undefined;

  public constructor(input?: {trace?: boolean}) {
    this.trace = input?.trace === true;
  }

  public async connect() {
    this.sqlite = new DatabaseSync(":memory:");

    // @ts-ignore
    if (abap?.context?.databaseConnections && abap.context.databaseConnections["DEFAULT"] === this) {
      // @ts-ignore
      abap.builtin.sy.get().dbsys?.set(this.name);
    }
  }

  public async disconnect() {
    this.sqlite!.close();
    this.sqlite = undefined;
  }

  public async execute(sql: string | string[]): Promise<void> {
    if (typeof sql === "string") {
      if (sql === "") {
        return;
      }
      this.sqlite!.exec(sql);
    } else {
      for (const s of sql) {
        await this.execute(s);
      }
    }
  }

  public export() {
    return; // todo
  }

  public async beginTransaction() {
    return; // todo
  }

  public async commit() {
    return; // todo
  }

  public async rollback() {
    return; // todo
  }

  public async delete(options: DB.DeleteDatabaseOptions) {
    const sql = `DELETE FROM ${options.table} WHERE ${options.where}`;

    let subrc = 0;
    let dbcnt = 0;
    try {
      if (this.trace === true) {
        console.log(sql);
      }

      const stm = this.sqlite!.prepare(sql);
      stm.setReadBigInts(false);
      const res = stm.run();
      dbcnt = res.changes as number;

      if (dbcnt === 0) {
        subrc = 4;
      }
    } catch (error) {
      subrc = 4;
    }

    return {subrc, dbcnt};
  }

  public async update(options: DB.UpdateDatabaseOptions) {
    const sql = `UPDATE ${options.table} SET ${options.set.join(", ")} WHERE ${options.where}`;

    let subrc = 0;
    let dbcnt = 0;
    try {
      if (this.trace === true) {
        console.log(sql);
      }

      const stm = this.sqlite!.prepare(sql);
      stm.setReadBigInts(false);
      const res = stm.run();
      dbcnt = res.changes as number;

      if (dbcnt === 0) {
        subrc = 4;
      }
    } catch (error) {
      subrc = 4;
    }

    return {subrc, dbcnt};
  }

  public async insert(options: DB.InsertDatabaseOptions) {
    const sql = `INSERT INTO ${options.table} (${options.columns.map(c => "'" + c + "'").join(",")}) VALUES (${options.values.join(",")})`;

    let subrc = 0;
    let dbcnt = 0;
    try {
      if (this.trace === true) {
        console.log(sql);
      }

      this.sqlite!.exec(sql);
      dbcnt = 1;
    } catch (error) {
      if (this.trace === true) {
        console.dir(error);
      }
      // eg "UNIQUE constraint failed" errors
      subrc = 4;
    }
    return {subrc, dbcnt};
  }

  // // https://www.sqlite.org/lang_select.html
  public async select(options: DB.SelectDatabaseOptions) {
    let rows: undefined | DB.DatabaseRows = undefined;

    options.select = options.select.replace(/ UP TO (\d+) ROWS(.*)/i, "$2 LIMIT $1");
    if (options.primaryKey) {
      options.select = options.select.replace(/ ORDER BY PRIMARY KEY/i, " ORDER BY " + options.primaryKey.join(", "));
    } else {
      options.select = options.select.replace(/ ORDER BY PRIMARY KEY/i, "");
    }
    options.select = options.select.replace(/ ASCENDING/ig, " ASC");
    options.select = options.select.replace(/ DESCENDING/ig, " DESC");
    options.select = options.select.replace(/~/g, ".");

    if (this.trace === true) {
      console.log(options.select);
    }

    try {
      const stm = this.sqlite!.prepare(options.select);
      rows = stm.all() as DB.DatabaseRows;
    } catch (error) {
      // @ts-ignore
      if (abap.Classes["CX_SY_DYNAMIC_OSQL_SEMANTICS"] !== undefined) {
        // @ts-ignore
        throw await new abap.Classes["CX_SY_DYNAMIC_OSQL_SEMANTICS"]().constructor_({sqlmsg: error.message || ""});
      }
      throw error;
    }

    return {rows};
  }

  public async openCursor(options: DB.SelectDatabaseOptions): Promise<DB.DatabaseCursorCallbacks> {
    const {rows} = await this.select(options);
    let index = 0;
    return {
      fetchNextCursor: async (packageSize: number) => {
        const pkg = rows.slice(index, index + packageSize);
        index += packageSize;
        return {rows: pkg};
      },
      closeCursor: async () => {
        // not necessary
      },
    };
  }

}