interface condition {
    "condition-index": number,
    using: "and" | "or"
}
export interface filter {
    name: string,
    operator: string,
    values: Array<any>,
    combine: condition
}

interface partitionKey {
    range: number
}
interface filterable {
    sorted: string
}
export interface definition {
    name: string,
    datatype: string,
    filterable: filterable,
    primary: boolean,
    key: partitionKey
}