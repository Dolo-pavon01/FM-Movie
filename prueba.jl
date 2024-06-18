function sum_single(a)
    s = []
    for i in a
        push!(s, a)
    end
    return s
end



function sum_multi_good(a)
    chunks = Iterators.partition(a, length(a) ÷ Threads.nthreads())
    tasks = map(chunks) do chunk
        Threads.@spawn sum_single(chunk)
    end
    chunk_sums = fetch.(tasks)
    println(sum_single(chunk_sums))
end

sum_multi_good([Dict(), Dict(), Dict(), Dict(), Dict(), Dict(), Dict(), Dict()])
