import gleam/option

pub type Job {
  Job(
    id: String,
    queue: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    reserved_at: option.Option(#(#(Int, Int, Int), #(Int, Int, Int))),
  )
}

pub type SucceededJob {
  SucceededJob(
    id: String,
    queue: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    succeded_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}

pub type FailedJob {
  FailedJob(
    id: String,
    queue: String,
    name: String,
    payload: String,
    attempts: Int,
    exception: String,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    failed_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}
