using Random
using LinearAlgebra

Random.seed!(912)

M = BigInt.(rand(Int128, 150, 150));

println("Tengo una matriz de:", size(M, 1), "x", size(M,2))

nthreads = Threads.nthreads(); #configurar .json en vscode o correr con "julia --threads n th_matrix_mult.jl"

#=
for i in 1:nthreads
    println("i: ", i, "\t Thread ID: ", Threads.threadid())
end

Threads.@threads for i in 1:nthreads
    println("i: ", i, "\t Thread ID: ", Threads.threadid())
end
=#

function mul_th(A::AbstractMatrix, B::AbstractMatrix)
    C = similar(A, size(A,1), size(B,2)) #matriz inicializada de nxm, siendo n la # de columnas de A y m la # de filas de B
    Threads.@threads for i in axes(A,1)
        for j in axes(B,2)
            acc = zero(eltype(C)) #inicializa acc con el 0 del tipo de C (en nuestro caso, el 0 de BigInt)
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

print("multiplicacion de matrices con concurrencia, con $nthreads threads:\n")
M4 == @time mul_th(mul_th(M, M), mul_th(M, M))