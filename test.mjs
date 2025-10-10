import { DataFactory } from "rdf-data-factory";

const DF = new DataFactory();

let a = DF.fromTerm({value:"aaa", termType: "NamedNode"});
console.log(a);
console.log(a.equals(a));