import {expect} from "chai";
import {Transpiler} from "../src";
import * as abaplint from "@abaplint/core";
import {IFile} from "../src/types";

async function runFiles(files: IFile[]) {
  const memory = files.map(f => new abaplint.MemoryFile(f.filename, f.contents));
  const reg: abaplint.IRegistry = new abaplint.Registry().addFiles(memory).parse();
  const res = await new Transpiler().run(reg);
  return res.objects;
}

const t000 = `<?xml version="1.0" encoding="utf-8"?>
<abapGit version="v1.0.0" serializer="LCL_OBJECT_TABL" serializer_version="v1.0.0">
 <asx:abap xmlns:asx="http://www.sap.com/abapxml" version="1.0">
  <asx:values>
   <DD02V>
    <TABNAME>T000</TABNAME>
    <DDLANGUAGE>E</DDLANGUAGE>
    <TABCLASS>TRANSP</TABCLASS>
    <DDTEXT>T000</DDTEXT>
    <CONTFLAG>A</CONTFLAG>
    <EXCLASS>1</EXCLASS>
   </DD02V>
   <DD09L>
    <TABNAME>T000</TABNAME>
    <AS4LOCAL>A</AS4LOCAL>
    <TABKAT>0</TABKAT>
    <TABART>APPL0</TABART>
    <BUFALLOW>N</BUFALLOW>
   </DD09L>
   <DD03P_TABLE>
    <DD03P>
     <TABNAME>T000</TABNAME>
     <FIELDNAME>MANDT</FIELDNAME>
     <DDLANGUAGE>E</DDLANGUAGE>
     <POSITION>0001</POSITION>
     <KEYFLAG>X</KEYFLAG>
     <ADMINFIELD>0</ADMINFIELD>
     <INTTYPE>C</INTTYPE>
     <INTLEN>000006</INTLEN>
     <NOTNULL>X</NOTNULL>
     <DATATYPE>CHAR</DATATYPE>
     <LENG>000003</LENG>
     <MASK>  CHAR</MASK>
    </DD03P>
    <DD03P>
     <TABNAME>T000</TABNAME>
     <FIELDNAME>CCCATEGORY</FIELDNAME>
     <DDLANGUAGE>E</DDLANGUAGE>
     <POSITION>0002</POSITION>
     <ADMINFIELD>0</ADMINFIELD>
     <INTTYPE>C</INTTYPE>
     <INTLEN>000002</INTLEN>
     <DATATYPE>CHAR</DATATYPE>
     <LENG>000001</LENG>
     <MASK>  CHAR</MASK>
    </DD03P>
    <DD03P>
     <TABNAME>T000</TABNAME>
     <FIELDNAME>CCNOCLIIND</FIELDNAME>
     <DDLANGUAGE>E</DDLANGUAGE>
     <POSITION>0003</POSITION>
     <ADMINFIELD>0</ADMINFIELD>
     <INTTYPE>C</INTTYPE>
     <INTLEN>000002</INTLEN>
     <DATATYPE>CHAR</DATATYPE>
     <LENG>000001</LENG>
     <MASK>  CHAR</MASK>
    </DD03P>
   </DD03P_TABLE>
  </asx:values>
 </asx:abap>
</abapGit>`;

describe("Files", () => {

  it("Two reports", async () => {
    const file1 = {filename: "zfoo1.prog.abap", contents: "WRITE '1'."};
    const file2 = {filename: "zfoo2.prog.abap", contents: "WRITE '2'."};

    const output = await runFiles([file1, file2]);

    expect(output.length).to.equal(2);
    expect(output[0].filename).to.equal("zfoo1.prog.mjs");
    expect(output[1].filename).to.equal("zfoo2.prog.mjs");
  });

  it("Full path file name", async () => {
    const filename = "C:\\Users\\foobar\\git\\transpiler\\packages\\abap-loader\\build\\test\\zprogram.prog.abap";
    const file1 = {filename, contents: "WRITE '1'."};

    const output = await runFiles([file1]);

    expect(output.length).to.equal(1);
    expect(output[0].filename).to.contain("zprogram.prog.mjs");
  });

  it("Global Class", async () => {
    const filename = "zcl_index.clas.abap";
    const contents = `
CLASS zcl_index DEFINITION PUBLIC.
ENDCLASS.
CLASS zcl_index IMPLEMENTATION.
ENDCLASS.
`;
    const file1 = {filename, contents};

    const output = await runFiles([file1]);

    expect(output.length).to.equal(1);
    expect(output[0].chunk.getCode()).to.include("zcl_index");
    expect(output[0].exports.length).to.equal(1);
    expect(output[0].exports[0]).to.equal("zcl_index");
  });

  it("Global Class and testclasses", async () => {
    const filename1 = "zcl_index.clas.abap";
    const contents1 = `
CLASS zcl_index DEFINITION PUBLIC.
ENDCLASS.
CLASS zcl_index IMPLEMENTATION.
ENDCLASS.
`;
    const file1 = {filename: filename1, contents: contents1};

    const filename2 = "zcl_index.clas.testclasses.abap";
    const contents2 = `
    CLASS ltcl_test DEFINITION FOR TESTING DURATION SHORT RISK LEVEL HARMLESS FINAL.
    PRIVATE SECTION.
      METHODS test FOR TESTING.
  ENDCLASS.

  CLASS ltcl_test IMPLEMENTATION.
    METHOD test.
      DATA foo TYPE REF TO zcl_index.
      CREATE OBJECT foo.
    ENDMETHOD.
  ENDCLASS.
`;
    const file2 = {filename: filename2, contents: contents2};

    const output = await runFiles([file1, file2]);

    expect(output.length).to.equal(2);
    expect(output[0].chunk.getCode()).to.include("zcl_index");
    expect(output[0].exports.length).to.equal(1, "one export expected, global class");
    expect(output[0].requires.length).to.equal(0, "no requires from global class");

    expect(output[1].chunk.getCode()).to.include("ltcl_test");
    expect(output[1].exports.length).to.equal(1, "one export expected, testclass");
    expect(output[1].requires.length).to.equal(1); // the global class
  });

  it("Global Class implementing global intf", async () => {
    const filename1 = "zcl_index.clas.abap";
    const contents1 = `
CLASS zcl_index DEFINITION PUBLIC.
  PUBLIC SECTION.
    INTERFACES zif_index.
ENDCLASS.
CLASS zcl_index IMPLEMENTATION.
  METHOD zif_index~bar.
  ENDMETHOD.
ENDCLASS.`;

    const filename2 = "zif_index.intf.abap";
    const contents2 = `
INTERFACE zif_index PUBLIC.
  METHODS bar IMPORTING foo TYPE i DEFAULT 25.
ENDINTERFACE.`;

    const file1 = {filename: filename1, contents: contents1};
    const file2 = {filename: filename2, contents: contents2};

    const output = await runFiles([file1, file2]);

    expect(output.length).to.equal(2);
    expect(output[0].chunk.getCode()).to.include("zcl_index");
    expect(output[0].chunk.getCode()).to.include("const constant_25 = ");
  });

  it("Global Class implementing global intf, whitespace lookup default parameter value", async () => {
    const filename1 = "zcl_index.clas.abap";
    const contents1 = `
CLASS zcl_index DEFINITION PUBLIC.
  PUBLIC SECTION.
    INTERFACES zif_index.
ENDCLASS.



CLASS zcl_index IMPLEMENTATION.
  METHOD zif_index~bar.
  ENDMETHOD.
ENDCLASS.`;

    const filename2 = "zif_index.intf.abap";
// note how the method is defined at the postion between ENDCLASS and CLASS IMPLEMENTAION above,
    const contents2 = `
INTERFACE zif_index PUBLIC.



  METHODS bar IMPORTING foo TYPE abap_bool DEFAULT abap_true.
ENDINTERFACE.`;

    const file1 = {filename: filename1, contents: contents1};
    const file2 = {filename: filename2, contents: contents2};

    const output = await runFiles([file1, file2]);

    expect(output.length).to.equal(2);
  });

  it("Global TABL", async () => {
    const file1 = {filename: "t000.tabl.xml", contents: t000};
    const output = await runFiles([file1]);
    expect(output.length).to.equal(1);
    expect(output[0].chunk.getCode()).to.include("T000");
  });

  it("Global INTF + CLAS, static method from interface", async () => {
    const filename1 = "if_system_uuid_rfc4122_static.intf.abap";
    const contents1 = `INTERFACE if_system_uuid_rfc4122_static PUBLIC.
  CLASS-METHODS create_uuid_c36_by_version
    IMPORTING
      version TYPE i
    RETURNING
      VALUE(uuid) TYPE string.
ENDINTERFACE.`;
    const file1 = {filename: filename1, contents: contents1};

    const filename2 = "cl_system_uuid.clas.abap";
    const contents2 = `CLASS cl_system_uuid DEFINITION PUBLIC.
    PUBLIC SECTION.
      INTERFACES if_system_uuid_rfc4122_static.
  ENDCLASS.
  CLASS cl_system_uuid IMPLEMENTATION.
    METHOD if_system_uuid_rfc4122_static~create_uuid_c36_by_version.
      WRITE 'hello'.
    ENDMETHOD.
  ENDCLASS.`;
    const file2 = {filename: filename2, contents: contents2};

    const output = await runFiles([file1, file2]);

    expect(output.length).to.equal(2);
    const code = output[1].chunk.getCode();
    expect(code).to.include("static async");
  });

});