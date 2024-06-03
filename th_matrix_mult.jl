using Random
using LinearAlgebra

Random.seed!(912)

M = BigInt.(rand(Int128, 200, 200));

println("Tengo una matriz de:", size(M, 1), "x", size(M,2))

nthreads = Threads.nthreads(); #configurar .json en vscode!

for i in 1:nthreads
    println("i: ", i, "\t Thread ID: ", Threads.threadid())
end # chequear que efectivamente tengo nthreads

Threads.@threads for i in 1:nthreads
    println("i: ", i, "\t Thread ID: ", Threads.threadid())
end # chequear que efectivamente tengo nthreads

function mul_th(A::AbstractMatrix, B::AbstractMatrix)

    C = similar(A, size(A,1), size(B,2)) #matriz inicializada de nxm, siendo n la # de columnas de A y m la # de filas de B
    Threads.@threads for i in axes(A,1)
        for j in axes(B,2)
            acc = zero(eltype(C))
            for k in axes(A,2)
                acc += A[i,k] * B[k,j]
            end
            C[i,j] = acc
        end
    end
    C
end

print("multiplicacion de matrices sin concurrencia:\n")
M4 = @time M^4;

print("multiplicacion de matrices con concurrencia, con $threads threads:\n")
M4 == @time mul_th(mul_th(M, M), mul_th(M, M))

