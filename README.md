# Database component for [Enorith](https://github.com/enorith/framework)


## Usage

```go
package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/enorith/database"
	"fmt"
	"github.com/enorith/database/rithythm"
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
        JoinWith("inner", "areas", func(clause *database.JoinClause) {
        clause.AndOn("area_id", "=", "areas.id")
        clause.AndWhereNotNull("area_id")
    }).AndWhereNest(func(builder *database.QueryBuilder) {
        builder.AndWhere("users.id", ">", 12)
    }).
        Select(database.Raw("count(users.id) as users_count"), "area_id", "areas.name").
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