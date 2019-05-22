# Database component for [Rith](https://github.com/CaoJiayuan/rith)


## Usage

```go
package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/CaoJiayuan/rithdb"
	"fmt"
	"github.com/CaoJiayuan/rithdb/rithythm"
)


type User struct {
	rithythm.Model
}

func (u *User) GetTable() string {
	return "users"
}

func (u *User) Clone() rithythm.DataModel {
	return &User{}
}

func main()  {
    builder := rithythm.Hold(&User{}).Query().GroupBy("area_id").
        JoinWith("inner", "areas", func(clause *rithdb.JoinClause) {
        clause.AndOn("area_id", "=", "areas.id")
        clause.AndWhereNotNull("area_id")
    }).AndWhereNest(func(builder *rithdb.QueryBuilder) {
        builder.AndWhere("users.id", ">", 12)
    }).
        Select(rithdb.Raw("count(users.id) as users_count"), "area_id", "areas.name").
        SortDesc("area_id")
    collection,_ := builder.Get()
    
    var (
        count int
        areaId int
        name string
    )
    
    defer collection.Close()
    
    for collection.NextAndScan(&count, &areaId, &name) {
        fmt.Println(count, areaId, name)
    }
}

```