package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/master"
	"github.com/cubefs/cubefs/proto"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/introspection"
	"io/ioutil"
	"os"
	"strings"
)

func main() {
	us := master.UserService{}
	parseSchema(us.Schema(), proto.AdminUserAPI, "user")

	//vs := master.VolumeService{}
	//parseSchema(vs.Schema(), proto.AdminVolumeAPI, "volume")

	cs := master.ClusterService{}
	parseSchema(cs.Schema(), proto.AdminClusterAPI, "cluster")

	fmt.Println("general success")
}

type Struct struct {
	name   string
	fields []*Field
}

func (s *Struct) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("type %s struct {", s.name))
	for _, f := range s.fields {
		buf.WriteString("\n" + f.FieldString())
	}
	buf.WriteString("\n}")
	return buf.String()
}

func (s *Struct) GraphqlFields(deep int) string {
	buf := &bytes.Buffer{}

	writeTable(buf, 0, "{\n")

	for _, f := range s.fields {
		if s, found := structMap[realKind(f.kind)]; found {
			writeTable(buf, deep+1, f.name)
			buf.WriteString(s.GraphqlFields(deep + 1))
		} else {
			writeTable(buf, deep+1, f.name+"\n")
		}
	}

	writeTable(buf, deep, "}\n")

	return buf.String()
}

func writeTable(buf *bytes.Buffer, deep int, value string) {
	for i := 0; i < deep; i++ {
		buf.WriteString("\t")
	}
	buf.WriteString(value)
}

type Field struct {
	name        string
	kind        string
	graphqlKind string
}

func (f *Field) FieldString() string {
	return "\t" + upFirst(f.name) + "\t" + f.kind
}

func realKind(kind string) string {
	if kind[0] == '*' {
		return realKind(kind[1:])
	} else if kind[0] == '[' {
		return realKind(kind[2:])
	} else {
		return kind
	}
}

type Function struct {
	name       string
	args       []Field
	returnType string
	methodType string
}

func (f *Function) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("func %s %s (ctx context.Context", methodBing, upFirst(upFirst(f.name))))

	for _, a := range f.args {
		buf.WriteString(", ")
		buf.WriteString(fixGoName(a.name) + " " + a.kind)
	}

	buf.WriteString(fmt.Sprintf(") (%s, error){\n", f.returnType))

	template := `
		req := client.NewRequest(ctx, |TEXT_CODE|%s(%s){
			%s%s%s
		}|TEXT_CODE|)
	
	%s
	
		rep, err := c.Query(ctx, "%s", req)
		if err != nil {
			return nil, err
		}
		result := %s
	
		if err := rep.GetValueByType(&result, "%s"); err != nil {
			return nil, err
		}
	
		return %sresult, nil
	`

	var returnBody string
	if s, found := structMap[realKind(f.returnType)]; !found {
		//panic(fmt.Sprintf("method:[%s] can return %s, %s", f.name, f.returnType, realKind(f.returnType)))
		println("fun:[%s] the return type %s must can make", f.name, f.returnType)
	} else {
		returnBody = s.GraphqlFields(3)
	}

	setVarBuf := bytes.Buffer{}
	for i, p := range f.args {
		setVarBuf.WriteString("\treq.Var(\"" + p.name + "\", " + fixGoName(p.name) + ")")
		if i != len(f.args)-1 {
			setVarBuf.WriteString("\n\t")
		}
	}
	setVar := setVarBuf.String()

	returnRef := ""
	if f.returnType[0] == '*' {
		returnRef = "&"
	}

	body := fmt.Sprintf(template, strings.ToLower(f.methodType), f.VarParam(), f.name, f.VarParamValue(), returnBody, setVar, url, makeIt(f.returnType), f.name, returnRef)

	buf.WriteString(strings.ReplaceAll(body, "|TEXT_CODE|", "`"))

	buf.WriteString("\n}")

	return buf.String()

}

func makeIt(name string) string {
	if name[0] == '*' {
		return makeIt(name[1:])
	}

	if name[0] == '[' {
		return fmt.Sprintf("make(%s,0)", name)
	}

	return fmt.Sprintf("%s{}", name)
}

func fixGoName(name string) string {
	if name == "type" {
		return upFirst(name)
	}
	return name
}

func (f *Function) VarParam() string {
	buf := bytes.Buffer{}
	for i, a := range f.args {
		buf.WriteString("$" + a.name + ": " + a.graphqlKind)
		if i != len(f.args)-1 {
			buf.WriteString(", ")
		}
	}
	return buf.String()
}

//for graphql query varparam
func (f *Function) VarParamValue() string {
	if len(f.args) == 0 {
		return ""
	}
	buf := bytes.Buffer{}
	buf.WriteString("(")
	for i, a := range f.args {
		buf.WriteString(a.name + ": $" + a.name)
		if i != len(f.args)-1 {
			buf.WriteString(", ")
		}

	}
	buf.WriteString(")")
	return buf.String()
}

var structMap map[string]Struct

// name kind
var functionMap map[string]Function

var url string

var outfile string

var source bytes.Buffer

var methodBing string

var hasTime bool

func parseSchema(schema *graphql.Schema, path, name string) {

	structMap = make(map[string]Struct)
	functionMap = make(map[string]Function)
	url = path

	methodBing = fmt.Sprintf("(c *%sClient)", upFirst(name))
	if err := os.MkdirAll("sdk/graphql/client/"+name, os.ModePerm); err != nil {
		panic(err)
	}
	outfile = "sdk/graphql/client/" + name + "/client.go"

	introspection.AddIntrospectionToSchema(schema)

	v, e := introspection.RunIntrospectionQuery(schema)
	if e != nil {
		panic(e)
	}

	m := make(map[string]interface{})

	if err := json.Unmarshal(v, &m); err != nil {
		panic(err)
	}
	types := m["__schema"].(map[string]interface{})["types"].([]interface{})

	for _, tp := range types {
		parse(tp.(map[string]interface{}))
	}

	makeSource(name)
	if e := ioutil.WriteFile(outfile, source.Bytes(), os.ModePerm); e != nil {
		panic(e)
	}
}

func parse(tp map[string]interface{}) {
	if tp["kind"] == "SCALAR" {
		return
	}

	if tp["name"] == "Query" || tp["name"] == "Mutation" {
		parseFunction(tp)
	} else {
		parseStruct(tp)
	}
}

func parseStruct(tp map[string]interface{}) {
	fields := tp["fields"].([]interface{})
	s := Struct{
		name:   tp["name"].(string),
		fields: make([]*Field, 0, len(fields)),
	}
	for _, f := range fields {
		field := f.(map[string]interface{})
		buf := bytes.Buffer{}
		makeType(&buf, field["type"].(map[string]interface{}), true)
		kind := buf.String()
		if kind == "Time" {
			hasTime = true
			kind = "time.Time"
		}
		s.fields = append(s.fields, &Field{
			name: field["name"].(string),
			kind: kind,
		})

	}
	structMap[s.name] = s
}

func makeGraphqlType(bf *bytes.Buffer, tp map[string]interface{}, none bool) {
	kind := tp["kind"].(string)
	if kind == "NON_NULL" {
		makeType(bf, tp["ofType"].(map[string]interface{}), false)
	} else if kind == "SCALAR" || kind == "OBJECT" {
		bf.WriteString(tp["name"].(string))
		if !none {
			bf.WriteString("!")
		}
		return
	} else if kind == "LIST" {
		bf.WriteString("[")
		makeType(bf, tp["ofType"].(map[string]interface{}), true)
		bf.WriteString("]")
		if !none {
			bf.WriteString("!")
		}
	}
}

func makeType(bf *bytes.Buffer, tp map[string]interface{}, none bool) {
	kind := tp["kind"].(string)
	if kind == "NON_NULL" {
		makeType(bf, tp["ofType"].(map[string]interface{}), false)
	} else if kind == "SCALAR" || kind == "OBJECT" {
		if none {
			bf.WriteString("*")
		}
		bf.WriteString(tp["name"].(string))
		return
	} else if kind == "LIST" {
		if none {
			bf.WriteString("*")
		}
		bf.WriteString("[]")
		makeType(bf, tp["ofType"].(map[string]interface{}), true)
	}
}

func parseFunction(tp map[string]interface{}) {
	fields := tp["fields"].([]interface{})

	for _, f := range fields {
		_parseFunction(f.(map[string]interface{}), tp["name"].(string))
	}

}

func _parseFunction(m map[string]interface{}, methodType string) {
	args := m["args"].([]interface{})
	fun := Function{
		name:       m["name"].(string),
		args:       make([]Field, 0, len(args)),
		methodType: methodType,
	}
	for _, a := range args {
		fun.args = append(fun.args, parseArgs(a.(map[string]interface{})))
	}
	returnBuf := bytes.Buffer{}
	makeReturnType(&returnBuf, m["type"].(map[string]interface{}), true)
	fun.returnType = returnBuf.String()

	functionMap[fun.name] = fun
}

func makeReturnType(buf *bytes.Buffer, tp map[string]interface{}, none bool) {
	kind := tp["kind"].(string)
	if kind == "NON_NULL" {
		makeReturnType(buf, tp["ofType"].(map[string]interface{}), false)
	} else if kind == "SCALAR" || kind == "OBJECT" {
		if none {
			buf.WriteString("*")
		}
		name := tp["name"].(string)
		buf.WriteString(name)
	} else if kind == "LIST" {
		if none {
			buf.WriteString("*")
		}
		buf.WriteString("[]")
		makeReturnType(buf, tp["ofType"].(map[string]interface{}), true)
	} else {
		panic(fmt.Sprintf("can do this for kind:[%s]", kind))
	}

}

func parseArgs(m map[string]interface{}) Field {
	buf := bytes.Buffer{}
	makeType(&buf, m["type"].(map[string]interface{}), true)

	graphqlBuf := bytes.Buffer{}
	makeGraphqlType(&graphqlBuf, m["type"].(map[string]interface{}), true)

	return Field{
		name:        m["name"].(string),
		kind:        buf.String(),
		graphqlKind: graphqlBuf.String(),
	}
}

func upFirst(s string) string {
	u := strings.ToUpper(s)
	return u[:1] + s[1:]
}

func makeSource(name string) {
	source = bytes.Buffer{}

	source.WriteString("package " + name + "\n")

	head := `//auto generral by sdk/graphql general.go

import "context"
import "github.com/cubefs/cubefs/sdk/graphql/client"
`

	if hasTime {
		head += `import "time"`
	}

	head += `

type %sClient struct {
	*client.MasterGClient
}

func New%sClient(c *client.MasterGClient) *%sClient {
	return &%sClient{c}
}

`
	source.WriteString(fmt.Sprintf(head, upFirst(name), upFirst(name), upFirst(name), upFirst(name)))

	source.WriteString("//struct begin .....\n")
	for _, s := range structMap {
		source.WriteString(s.String())
		source.WriteString("\n\n")
	}

	source.WriteString("//function begin .....\n")
	for _, f := range functionMap {
		source.WriteString(f.String())
		source.WriteString("\n\n")
	}

}
